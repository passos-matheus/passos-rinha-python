import os
import asyncio

from rinha.models.models import PaymentDatabase
from rinha.queue.redis_queue import RedisQueue
from rinha.workers.payment_worker import PaymentWorker, WorkerConfig
from rinha.payment_processors.principal_payment_processor import PrincipalPaymentProcessor
from rinha.payment_processors.fallback_payment_processor import FallbackPaymentProcessor
from httpx import AsyncClient
from rinha.persistence.redis_database import RedisDatabase


async def main():
    max_connections: int = int(os.getenv('HTTPX_MAX_CONNECTIONS', '100'))
    base_url: str = os.getenv('GATEWAYS_BASE_URL', '123 n lembro')


    async_client: AsyncClient = AsyncClient(
        base_url=base_url,
        limits=max_connections
    )

    db: PaymentDatabase = RedisDatabase(async_client=async_client)
    main_processor = PrincipalPaymentProcessor(async_client=async_client, db=db)
    fallback_processor = FallbackPaymentProcessor(async_client=async_client, db=db)
    
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
