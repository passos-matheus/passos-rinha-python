from typing import Optional
from fastapi import FastAPI
from datetime import datetime
from fastapi.params import Query
from rinha.queue.redis_queue import RedisQueue
from rinha.persistence.redis_database import RedisDatabase
from rinha.models.models import (
    Payment, 
    PaymentQueue,
    PaymentDatabase,
    PaymentsSummary,
)


payment_queue: PaymentQueue = RedisQueue()
payments_database: PaymentDatabase = RedisDatabase()

app = FastAPI()


@app.post("/payments")
async def payments(payment: Payment):
    inserted = await payment_queue._insert_on_queue(payment)

    return {
        "message": inserted,
        "correlationId": payment.correlationId,
        "amount": payment.amount,
    }

@app.get("/payments-summary")
async def get_payments_summary(
    from_: Optional[datetime] = Query(None, alias="from"),
    to: Optional[datetime] = Query(None),
):
    
    _default, _fallback = await payments_database.get_payments_summary()
    
    return PaymentsSummary(
        default=_default,
        fallback=_fallback
    )
    