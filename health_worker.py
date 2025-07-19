import asyncio
import json

from httpx import AsyncClient, Limits, Timeout
from redis.asyncio import Redis

from models.models import PaymentDatabase, PaymentProcessorStatus
from persistence.redis_database import RedisDatabase
from workers.health_checker_worker import HealthCheckerWorker


async def main():
    async_client: AsyncClient = AsyncClient(
        timeout=Timeout(read=0.5, write=0.5, connect=0.5, pool=None),
        limits=Limits(
            max_connections=30,
            max_keepalive_connections=50
        )
    )

    redis_client = Redis(
        host='localhost',
        port=6379,
        decode_responses=True,
        socket_timeout=5,
    )

    db: RedisDatabase = RedisDatabase(
        redis_client=redis_client
    )

    t = await db.redis_client.initialize()
    teste = await db.redis_client.ping()
    print(t)
    print(teste)

    worker = HealthCheckerWorker(async_client=async_client, db=db)

    await worker.run()



if __name__ == '__main__':
    asyncio.run(main())

