from typing import Optional
from fastapi import FastAPI
from datetime import datetime
from fastapi.params import Query
from redis.asyncio import Redis
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
    queue_name='payments-queue',
    redis_client=redis_client
)

app = FastAPI()

@app.post("/payments")
async def payments(payment: Payment):
    try:
        await queue.insert_on_queue(payment)

        return {
            "message": "Pagamento enfileirado com sucesso",
            "correlationId": payment.correlationId,
            "amount": payment.amount,
        }
    except:
        raise

@app.get("/payments-summary")
async def get_payments_summary(
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
):

    _default, _fallback = await db.get_payments_summary()

    return PaymentsSummary(
        default=_default,
        fallback=_fallback
    )
