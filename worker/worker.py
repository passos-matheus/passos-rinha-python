import random
from pydantic import BaseSettings
from redis.asyncio import Redis
from models import PaymentQueue, PaymentProcessor, PaymentProcessorStatus

import asyncio

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)
import logging



logger = logging.getLogger(__name__)

class ExecutorError(Exception):
    pass



class Worker(BaseSettings):
    queue: PaymentQueue
    main: PaymentProcessor
    fallback: PaymentProcessor

    main_attempts: int = 3
    fallback_attempts: int = 2
    base_delay: float = 0.3
    max_delay: float = 2.5  
    per_try_timeout: float = 1.0

    async def worker(self):
        while True:
            
            get_from_top = False
            run_in_default_mode = True

            p1, p2 = await self.check_payments_health()

            if p1.failing and p1.minResponseTime <= p2.minResponseTime * 1.2:
                run_in_default_mode = True
                get_from_top = False

            if not p1.failing and p1.minResponseTime <= p2.minResponseTime * 1.2:
                run_in_default_mode = True
                get_from_top = False
            
                pass


        pass

    async def check_payments_health(self):
        principal: PaymentProcessorStatus = await self.principal_processor.check_health()
        fallback: PaymentProcessorStatus = await self.fallback_processor.check_health()

        principal_status = not principal.failing and principal.minResponseTime <= fallback.minResponseTime * 1.2
        fallback_status = not fallback.failing

    async def default_run(self, task):
        try:
            return await self._tenacity_run(
                operation=lambda: self._run_main(task),
                attempts=self.main_attempts,
                name="main",
            )
        except ExecutorError as e:
            logger.warning(
                "nain falhou após %d tentativas: %s — partindo para fallback", 
                self.main_attempts, 
                e
            )

        return await self._tenacity_run(
            operation=lambda: self._run_fallback(task),
            attempts=self.fallback_attempts,
            name="fallback",
        )
    
    async def fallback_run(self, task):
        try:
            return await self._tenacity_run(
                operation=lambda: self._run_fallback(task),
                attempts=self.main_attempts,
                name="fallback",
            )
        except ExecutorError as e:
            logger.warning(
                "main não saúdavel e fallback falhou após %d tentativas: %s", 
                self.main_attempts, 
                e
            )
            raise e
        

    async def _tenacity_run(self, operation, attempts: int, name: str):
        retryer = AsyncRetrying(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential_jitter(initial=self.base_delay, max=self.max_delay),
            retry=retry_if_exception_type(ExecutorError),
            reraise=True,
            before_sleep=lambda retry_state: logger.debug(
                "%s: tentativa %d falhou, esperando %s segundos",
                name,
                retry_state.attempt_number,
                retry_state.next_action.sleep,
            ),
        )

        async for attempt in retryer:
            with attempt:
                return await operation()

    async def _run_main(self, task):
        resp = await self.main.execute(task)
        if resp.status_code != 200:
            raise ExecutorError(f"Status {resp.status_code} no main")
        
        return resp

    async def _run_fallback(self, task):
        resp = await self.fallback.execute(task)
        if resp.status_code != 200:
            raise ExecutorError(f"Status {resp.status_code} no fallback")
        
        return resp
