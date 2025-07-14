import asyncio

from rinha.queue.redis_queue import RedisQueue
from rinha.models.models import PaymentProcessor
from rinha.workers.payment_worker import PaymentWorker, WorkerConfig
from rinha.payment_processors.principal_payment_processor import PrincipalPaymentProcessor
from rinha.payment_processors.fallback_payment_processor import FallbackPaymentProcessor


async def main():
    main_processor = PrincipalPaymentProcessor()
    fallback_processor = FallbackPaymentProcessor()
    queue = RedisQueue()
    config = WorkerConfig()

    worker = PaymentWorker(
        main=main_processor,
        fallback=fallback_processor,
        queue=queue,
        cfg=config
    )

    await worker.run()

if __name__ == '__main__':
    asyncio.run(main())
