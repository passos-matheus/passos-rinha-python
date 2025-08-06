import asyncio
from nats.aio.client import Client

queue = asyncio.Queue(maxsize=5000)
NUM_WORKERS = 4

async def consume_nats():
    nc = Client()
    await nc.connect("nats://nats:5000")

    async def put_on_queue_callback(msg):
        try:
            await queue.put(msg.data)
        except asyncio.CancelledError:
            raise

    await nc.subscribe("fila.rinha", cb=put_on_queue_callback)

async def process_queue(worker_id):
    while True:
        payload = await queue.get()
        try:
            await process_payment(payload)
        except Exception as e:
            print(f"[worker-{worker_id}] Erro: {e}")
        finally:
            queue.task_done()

async def process_payment(payment):
    try:
        print(payment)
    except Exception as e:
        print(e)
        await queue.put(payment)

async def main():
    consumers = [process_queue(i) for i in range(NUM_WORKERS)]
    await asyncio.gather(
        consume_nats(),
        *consumers
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nEncerrando...")
