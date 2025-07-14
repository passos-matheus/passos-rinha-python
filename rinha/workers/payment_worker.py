import asyncio
import logging

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)
from pydantic import BaseModel, BaseSettings, Field
from typing import Any, Callable, Awaitable, TypeVar
from rinha.models.models import PaymentQueue, PaymentProcessor, PaymentProcessorStatus


logger = logging.getLogger(__name__)
T = TypeVar("T")


class ExecutorError(Exception):
    pass


class Strategy(BaseModel):
    take_from_top: bool
    start_with_main: bool


class WorkerConfig(BaseSettings):
    main_attempts: int = 3
    fallback_attempts: int = 2
    base_delay: float = 0.3
    max_delay: float = 2.5
    per_try_timeout: float = 1.0

    class Config:
        env_prefix = "WORKER_"


class PaymentWorker(BaseModel):
    queue: PaymentQueue
    main: PaymentProcessor
    fallback: PaymentProcessor
    cfg: WorkerConfig = Field(_default_factory=lambda: WorkerConfig())

    class Config:
        arbitraty_types = True

    async def run(self) -> None:
        while True:
            try:
                strategy = await self._determine_strategy()
                payment = await self._get_next_payment(strategy)
                await self._process_payment(payment, strategy)
            except asyncio.CancelledError:
                logger.info("Worker cancelado, encerrando loop.")
                break
            except Exception:
                logger.exception("Erro inesperado no worker")

    async def _determine_strategy(self) -> Strategy:
        p1, p2 = await self.check_payments_health()
        
        main_ok = not p1.failing
        latency_ok = p1.minResponseTime <= p2.minResponseTime * 1.2

        if main_ok and latency_ok:
            return Strategy(take_from_top=True, start_with_main=True)
        
        if main_ok and not latency_ok:
            return Strategy(take_from_top=False, start_with_main=False)
        
        if not main_ok and latency_ok:
            return Strategy(take_from_top=False, start_with_main=True)
        
        return Strategy(take_from_top=True, start_with_main=False)

    async def _get_next_payment(self, strategy: Strategy) -> Any:
        if strategy.take_from_top:
            return await self.queue.get_from_top()
        return await self.queue.get_from_bottom()

    async def _process_payment(self, payment: Any, strategy: Strategy) -> None:
        if strategy.start_with_main:
            try:
                await self._retry(
                    lambda: self.main.execute(payment),
                    attempts=self.cfg.main_attempts,
                    name="main"
                )
                return
            except ExecutorError as e:
                logger.warning("Main falhou: %s — partindo para fallback", e)

        await self._retry(
            lambda: self.fallback.execute(payment),
            attempts=self.cfg.fallback_attempts,
            name="fallback"
        )

    async def _retry(self, fn: Callable[[], Awaitable[T]], *, attempts: int, name: str):
        retryer = AsyncRetrying(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential_jitter(
                initial=self.cfg.base_delay,
                max=self.cfg.max_delay
            ),
            retry=retry_if_exception_type(ExecutorError),
            reraise=True,
            before_sleep=lambda state: logger.debug(
                "%s: tentativa %d falhou, esperando %.2fs",
                name, state.attempt_number, state.next_action.sleep
            ),
        )

        async for attempt in retryer:
            with attempt:
                return await asyncio.wait_for(fn(), timeout=self.cfg.per_try_timeout)

    async def check_payments_health(self) -> tuple[PaymentProcessorStatus, PaymentProcessorStatus]:
        p1 = await self.main.check_health()
        p2 = await self.fallback.check_health()
        return p1, p2

