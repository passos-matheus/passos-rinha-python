import os
import asyncio
import random
import uuid

from httpx import AsyncClient, Limits, Timeout
from redis.asyncio.connection import ConnectionPool

from rinha.queue.redis_queue import RedisQueue
from rinha.models.models import PaymentDatabase, PaymentProcessor, Payment
from rinha.persistence.redis_database import RedisDatabase
from rinha.workers.payment_worker import PaymentWorker, WorkerConfig
from rinha.payment_processors.fallback_payment_processor import FallbackPaymentProcessor
from rinha.payment_processors.principal_payment_processor import PrincipalPaymentProcessor
from redis.asyncio import Redis


async def main():
    redis_pool = ConnectionPool(
        host=os.getenv("REDIS_HOST", "redis"),
        port=6379,
        max_connections=5000,
        decode_responses=True,
        socket_timeout=5
    )

    redis_client = Redis(connection_pool=redis_pool)

    async_client: AsyncClient = AsyncClient(
        timeout=Timeout(read=0.5, write=0.5, connect=0.5, pool=None),
        limits=Limits(
            max_connections=5000,
            max_keepalive_connections=2000
        )
    )

    db: PaymentDatabase = RedisDatabase(redis_client=redis_client)

    main_processor: PaymentProcessor = PrincipalPaymentProcessor(
        endpoint='http://payment-processor-default:8080/payments',
        async_client=async_client,
        db=db
    )

    fallback_processor = FallbackPaymentProcessor(
        endpoint='http://payment-processor-fallback:8080/payments',
        async_client=async_client,
        db=db
    )

    queue = RedisQueue(
        redis_client=redis_client,
        queue_name='payment_queue'
    )

    config = WorkerConfig()

    workers = [
        PaymentWorker(main=main_processor, fallback=fallback_processor, queue=queue, cfg=config, db=db)
        for _ in range(10)
    ]

    await asyncio.gather(*(worker.run() for worker in workers))


if __name__ == '__main__':
    asyncio.run(main())
