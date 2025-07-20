from typing import Optional
from fastapi import FastAPI, Depends
from datetime import datetime
from redis.asyncio import Redis

from rinha.models.models import PaymentsSummaryFilter
from rinha.queue.redis_queue import RedisQueue
from rinha.persistence.redis_database import RedisDatabase
from rinha.models.models import (
    Payment,
    PaymentQueue,
    PaymentDatabase,
    PaymentsSummary,
)


redis_client = Redis(
    host='localhost',
    port=6379,
    decode_responses=True,
    socket_timeout=5,
)

db: PaymentDatabase = RedisDatabase(redis_client=redis_client)

queue = RedisQueue(
    queue_name='payment_queue',
    redis_client=redis_client
)

app = FastAPI()

@app.post("/payments")
async def payments(payment: Payment):
    try:
        await queue.insert_on_queue(payment)
        response_dict: dict = {
            "message": "Pagamento enfileirado com sucesso",
            "correlationId": payment.correlationId,
            "amount": payment.amount,
        }

        return response_dict
    except:
        raise

@app.get("/payments-summary")
async def get_payments_summary(payment_filter: PaymentsSummaryFilter = Depends()):
    response = await db.get_payments_summary(
        from_str=payment_filter.date_from,
        to_str=payment_filter.date_to
    )

    print(response)
    return response
