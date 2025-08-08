import os
import json
import time
import asyncio

from datetime import datetime
from typing import Any

from utils.redis_client import redis_client
from payment_queue import queue
from health_checker import check_health_routine
from HTTP.aiohtpp_session import cleanup_session
from payment_processor import process_payment_in_default_processor, process_payment_in_fallback_processor

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

    best_processor = 1
    best_timeout = 5.1

    health_status = await redis_client.get('health_processors')
    if health_status:
        status = json.loads(health_status)
        best_processor = status["best"]
        # best_timeout = status["timeout"]

    async def run_with_semaphore(payment_json):
        async with semaphore:
            return await process_payment(payment_json, best_processor, best_timeout)

    tasks = [run_with_semaphore(payment) for payment in batch]
    await asyncio.gather(*tasks, return_exceptions=True)


async def process_payment(payment_json: Any, best_processor: int, best_timeout: float):
    max_retries = 3
    payment_processed = False
    processor_used = None

    try:
        payment = json.loads(payment_json)
        payload = {
            "correlationId": str(payment["correlationId"]),
            "amount": payment["amount"],
            "requestedAt": cached_datetime()
        }

        for attempt in range(max_retries):
            try:
                if best_processor == 1:
                    resp = await process_payment_in_default_processor(payload, best_timeout)
                    if resp == "ignored":
                        break
                    processor_used = "default"
                    payment_processed = True
                else:
                    resp = await process_payment_in_fallback_processor(payload, best_timeout)
                    if resp == "ignored":
                        break
                    processor_used = "fallback"
                    payment_processed = True

                break

            except Exception as e:
                error_str = str(e).lower()
                is_retryable = any(keyword in error_str for keyword in ['timeout', 'connection', 'refused'])

                if is_retryable and attempt < max_retries - 1:
                    await asyncio.sleep(0.1 * (attempt + 1))
                    continue
                elif is_retryable and attempt == max_retries - 1:
                    await queue.put(payment_json)
                else:
                    raise

        if payment_processed and processor_used:
            await save_payment(payload, processor_used)

    except (KeyError, ValueError, TypeError):
        return "parse_error"
    except Exception as e:
        print(e)
        await queue.put(payment_json)

async def save_payment(payload, key):
    if not key:
        raise Exception

    correlation_id = payload.get('correlationId')
    if not correlation_id:
        raise Exception("correlationId is required")

    processed_key = f"{key}:processed_ids"

    is_processed = await redis_client.sismember(processed_key, correlation_id)
    if is_processed:
        return

    pipeline = redis_client.pipeline()

    score = int(datetime.fromisoformat(payload['requestedAt']).timestamp() * 1000)
    member = json.dumps(payload)

    pipeline.zadd(key, {member: score})
    pipeline.sadd(processed_key, correlation_id)

    await pipeline.execute()

async def run_workers():
    try:
        consumers = [process_queue(i) for i in range(NUM_WORKERS)]
        await asyncio.gather(
            check_health_routine(),
            *consumers
        )
    finally:
        await cleanup_session()
