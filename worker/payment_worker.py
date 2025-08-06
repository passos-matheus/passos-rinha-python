import os
import json
import time
import asyncio

from datetime import datetime
from redis.asyncio.client import Redis
from payment_queue import consume_nats, queue
from HTTP.aiohtpp_session import cleanup_session
from redis.asyncio.connection import ConnectionPool
from payment_processor import process_payment_in_default_processor, process_payment_in_fallback_processor


redis_pool = ConnectionPool(
    host=os.getenv("REDIS_HOST", "redis-db"),
    port=6379,
    max_connections=100,
    decode_responses=True,
    socket_timeout=5
)
redis_client = Redis(connection_pool=redis_pool)

# 12980, duas instâncias
# NUM_WORKERS = 4
# CONCURRENT_REQUESTS = 6
# MAX_BATCH_SIZE = 250

NUM_WORKERS = 4
CONCURRENT_REQUESTS = 8
MAX_BATCH_SIZE = 250

semaphores = {}
_datetime_cache = datetime.now().isoformat()
_last_update = time.time()

def cached_datetime():
    global _datetime_cache, _last_update
    now = time.time()

    if now - _last_update > 0.05:
        _datetime_cache = datetime.now().isoformat()
        _last_update = now

    return _datetime_cache

async def process_queue(worker_id):
    semaphores[worker_id] = asyncio.Semaphore(CONCURRENT_REQUESTS)

    while True:
        batch = []

        item = await queue.get()
        batch.append(item)

        while len(batch) < MAX_BATCH_SIZE:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=0.008)
                batch.append(item)
            except asyncio.TimeoutError:
                break

        await process_batch(batch, worker_id)
        for _ in batch:
            queue.task_done()

async def process_batch(batch, worker_id):
    semaphore = semaphores[worker_id]

    async def run_with_semaphore(payment_json):
        async with semaphore:
            return await process_payment(payment_json)

    tasks = [run_with_semaphore(payment) for payment in batch]
    await asyncio.gather(*tasks, return_exceptions=True)

async def process_payment(payment_json):
    try:
        payment = json.loads(payment_json)
        payload = {
            "correlationId": str(payment["correlationId"]),
            "amount": payment["amount"],
            "requestedAt": cached_datetime()
        }

        await process_payment_in_default_processor(payload, 5.1)

        key = "default"
        score = int(datetime.fromisoformat(payload['requestedAt']).timestamp() * 1000)
        member = json.dumps(payload)

        await redis_client.zadd(key, {member: score})
    except (KeyError, ValueError, TypeError):
        return "parse_error"
    except Exception as e:
        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ['timeout', 'connection', 'refused']):
            await queue.put(payment_json)
        raise e

async def main():
    try:
        consumers = [process_queue(i) for i in range(NUM_WORKERS)]
        await asyncio.gather(
            consume_nats(),
            *consumers
        )
    finally:
        await cleanup_session()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nEncerrando...")
