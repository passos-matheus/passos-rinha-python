from fastapi import FastAPI
from models import (
    Payment, 
    PaymentQueue,
    PaymentDatabase,
    PaymentsSummary,
    PaymentProcessorSummary


)
from queue.redis_queue import RedisQueue

app = FastAPI()

payment_queue: PaymentQueue = RedisQueue(

)

payments_database: PaymentDatabase

@app.post("/payments")
async def payments(payment: Payment):
    
    inserted = await payment_queue._insert_on_queue(payment)

    return {
        "message": inserted,
        "correlationId": payment.correlationId,
        "amount": payment.amount,
    }

@app.get("payments-summary")
async def get_payments_summary():
    
    _default, _fallback = await payments_database.get_payments_summary()
    
    return PaymentsSummary(
        default=_default,
        fallback=_fallback
    )
    