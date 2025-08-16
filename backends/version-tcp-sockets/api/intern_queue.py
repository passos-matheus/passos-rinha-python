import json
import asyncio

from TCP.tcp_payment_queue_client import TCPQueueClient


NUM_WORKERS = 1
CONCURRENT_REQUESTS = 1
MAX_BATCH_SIZE = 250

semaphores = {}

async def process_queue(worker_id, _queue, tcp_client):
    semaphores[worker_id] = asyncio.Semaphore(CONCURRENT_REQUESTS)

    while True:
        batch = []

        item = await _queue.get()
        batch.append(item)

        while len(batch) < MAX_BATCH_SIZE:
            try:
                item = await asyncio.wait_for(_queue.get(), timeout=0.025)
                batch.append(item)
            except asyncio.TimeoutError:
                break

        await process_batch(batch, worker_id, _queue, tcp_client)
        for _ in batch:
            _queue.task_done()


async def process_batch(batch, worker_id, _queue, tcp_client):
    # semaphore = semaphores[worker_id]
    decoded_batch = [json.loads(item.decode("utf-8")) for item in batch]
    return await tcp_client.send_batch_payments(decoded_batch)


async def run_workers(tcp_client: TCPQueueClient, queue: asyncio.Queue):
    try:
        consumers = [process_queue(i, queue, tcp_client) for i in range(NUM_WORKERS)]
        await asyncio.gather(
            *consumers
        )

    except Exception as e:
        raise e
