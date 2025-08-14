import json
import time
import asyncio

from datetime import datetime
from typing import Any, List, Dict

from TCP.tcp_payment_queue_server import TCPQueueServer
from utils.redis_client import redis_client
from health_checker import check_health_routine
from HTTP.aiohtpp_session import cleanup_session
from payment_processor import process_payment_in_default_processor, process_payment_in_fallback_processor

redis_scripts = None

NUM_WORKERS = 4
CONCURRENT_REQUESTS = 8
MAX_BATCH_SIZE = 250

semaphores = {}
_datetime_cache = datetime.now().isoformat()
_last_update = time.time()


def init_redis_scripts(redis_client):
    global redis_scripts

    save_payment_script = redis_client.register_script("""
        local zset_key = KEYS[1]
        local processed_set_key = KEYS[2]
        local correlation_id = ARGV[1]
        local payload = ARGV[2]
        local score = ARGV[3]

        if redis.call('SISMEMBER', processed_set_key, correlation_id) == 1 then
            return 0  -- Já foi processado
        end

        redis.call('ZADD', zset_key, score, payload)
        redis.call('SADD', processed_set_key, correlation_id)

        return 1  -- Processado com sucesso
    """)

    batch_save_payments_script = redis_client.register_script("""
        local base_key = KEYS[1]
        local zset_key = base_key
        local processed_set_key = base_key .. ":processed_ids"
        local processed_count = 0
        local skipped_count = 0

        for i = 1, #ARGV, 3 do
            local correlation_id = ARGV[i]
            local payload = ARGV[i + 1]
            local score = ARGV[i + 2]

            if correlation_id and payload and score then
                if redis.call('SISMEMBER', processed_set_key, correlation_id) == 0 then
                    redis.call('ZADD', zset_key, score, payload)
                    redis.call('SADD', processed_set_key, correlation_id)
                    processed_count = processed_count + 1
                else
                    skipped_count = skipped_count + 1
                end
            end
        end
        return {processed_count, skipped_count}
    """)

    redis_scripts = {
        'save_payment': save_payment_script,
        'batch_save_payments': batch_save_payments_script
    }

def cached_datetime():
    global _datetime_cache, _last_update
    now = time.time()

    if now - _last_update > 0.05:
        _datetime_cache = datetime.now().isoformat()
        _last_update = now

    return _datetime_cache


async def process_queue(worker_id, _queue):
    semaphores[worker_id] = asyncio.Semaphore(CONCURRENT_REQUESTS)

    while True:
        batch = []

        item = await _queue.get()
        batch.append(item)

        while len(batch) < MAX_BATCH_SIZE:
            try:
                item = await asyncio.wait_for(_queue.get(), timeout=0.008)
                batch.append(item)
            except asyncio.TimeoutError:
                break

        await process_batch(batch, worker_id, _queue)
        for _ in batch:
            _queue.task_done()


async def process_batch(batch, worker_id, _queue):
    semaphore = semaphores[worker_id]

    best_processor = 1
    best_timeout = 5.1

    health_status = await redis_client.get('health_processors')
    if health_status:
        status = json.loads(health_status)

        if status['default']['failing']:
            await asyncio.sleep(5)

    async def run_with_semaphore(payment_json):
        async with semaphore:
            return await process_payment(payment_json, best_processor, best_timeout, _queue)

    tasks = [run_with_semaphore(payment) for payment in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    payments_to_save = []

    for result in results:
        if isinstance(result, dict) and result.get('save_data'):
            payments_to_save.append(result['save_data'])

    if payments_to_save:
        await batch_save_payments(payments_to_save)


async def process_payment(payment_json: Any, best_processor: int, best_timeout: float, _queue):
    print(f"executou o process_payment: {payment_json}")
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
                    await _queue.put(payment_json)
                else:
                    raise

        if payment_processed and processor_used:
            return {
                'save_data': {
                    'payload': payload,
                    'processor_used': processor_used
                }
            }

    except (KeyError, ValueError, TypeError):
        return "parse_error"
    except Exception as e:
        await _queue.put(payment_json)


async def batch_save_payments(payments_data: List[Dict]):
    if not payments_data:
        return

    grouped_payments = {}

    for payment_data in payments_data:
        payload = payment_data['payload']
        processor_used = payment_data['processor_used']

        correlation_id = payload.get('correlationId')
        if not correlation_id:
            continue

        if processor_used not in grouped_payments:
            grouped_payments[processor_used] = []

        score = int(datetime.fromisoformat(payload['requestedAt']).timestamp() * 1000)
        member = json.dumps(payload)

        grouped_payments[processor_used].extend([correlation_id, member, str(score)])

    for processor_key, argv_list in grouped_payments.items():
        if argv_list:
            await redis_scripts['batch_save_payments'](
                keys=[processor_key],
                args=argv_list
            )


async def save_payment(payload, key):
    if not key:
        raise Exception

    correlation_id = payload.get('correlationId')
    if not correlation_id:
        raise Exception("correlationId is required")

    processed_key = f"{key}:processed_ids"
    score = int(datetime.fromisoformat(payload['requestedAt']).timestamp() * 1000)
    member = json.dumps(payload)

    result = await redis_scripts['save_payment'](
        keys=[key, processed_key],
        args=[correlation_id, member, score]
    )
    return result == 1


async def run_workers():
    try:
        await redis_client.initialize()
        await redis_client.ping()

        init_redis_scripts(redis_client)

        queue = asyncio.Queue()
        tcp_server = TCPQueueServer(queue)
        tcp_server.start()

        consumers = [process_queue(i, queue) for i in range(NUM_WORKERS)]

        await asyncio.gather(
            check_health_routine(),
            *consumers
        )

    except Exception as e:
        raise e
    finally:
        try:
            await cleanup_session()
            await redis_client.close()
        except Exception as e:
            raise e

if __name__ == '__main__':
    asyncio.run(run_workers())
