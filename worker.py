import os
import asyncio
import random
import uuid

from httpx import AsyncClient, Limits, Timeout
from rinha.queue.redis_queue import RedisQueue
from rinha.models.models import PaymentDatabase, PaymentProcessor, Payment
from rinha.persistence.redis_database import RedisDatabase
from rinha.workers.payment_worker import PaymentWorker, WorkerConfig
from rinha.payment_processors.fallback_payment_processor import FallbackPaymentProcessor
from rinha.payment_processors.principal_payment_processor import PrincipalPaymentProcessor
from redis.asyncio import Redis

async def main():
    redis_client = Redis(
        host='localhost',
        port=6379,
        decode_responses=True,
        socket_timeout=5,
    )

    async_client: AsyncClient = AsyncClient(
        timeout=Timeout(read=0.5, write=0.5, connect=0.5, pool=None),
        limits=Limits(
            max_connections=30,
            max_keepalive_connections=50
        )
    )

    db: PaymentDatabase = RedisDatabase(
        redis_client=redis_client
    )

    main_processor: PaymentProcessor = PrincipalPaymentProcessor(
        endpoint='http://localhost:8001/payments',
        async_client=async_client,
        db=db
    )

    fallback_processor = FallbackPaymentProcessor(
        endpoint='http://localhost:8002/payments',
        async_client=async_client,
        db=db
    )
    
    queue = RedisQueue(
        queue_name='payment_queue',
        redis_client=redis_client
    )

    config = WorkerConfig()
    worker = PaymentWorker(
        main=main_processor,
        fallback=fallback_processor,
        queue=queue,
        cfg=config,
        db=db
    )
    payments: list[Payment] = [Payment(amount=random.uniform(1, 1000020), correlationId=uuid.uuid4()) for _ in range(0, 100)]
    for payment in payments:
        await queue.insert_on_queue(payment)


    await worker.run()

if __name__ == '__main__':
    asyncio.run(main())
