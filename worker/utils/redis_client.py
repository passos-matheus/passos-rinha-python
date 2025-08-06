import os

from redis.asyncio.client import Redis
from redis.asyncio.connection import ConnectionPool

redis_pool = ConnectionPool(
    host=os.getenv("REDIS_HOST", "redis-db"),
    port=6379,
    max_connections=100,
    decode_responses=True,
    socket_timeout=5
)

redis_client = Redis(connection_pool=redis_pool)