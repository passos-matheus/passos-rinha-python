import os
import asyncio

from nats.aio.client import Client

queue = asyncio.Queue(maxsize=50000)

async def consume_nats():
    nc = Client()
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    payments_subject = os.getenv("PAYMENTS_SUBJECT", "payments.queue")

    await nc.connect(nats_url)

    async def put_on_intern_queue_callback(msg):
        try:
            await queue.put(msg.data)
        except asyncio.CancelledError:
            raise

    await nc.subscribe(
        payments_subject,
        queue="payment-workers",
        cb=put_on_intern_queue_callback
    )

    print(f"Conectado ao NATS: {nats_url}. Subscrito em: {payments_subject} (queue group: payment-workers)")
