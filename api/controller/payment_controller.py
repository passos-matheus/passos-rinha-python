from models.payment import Payment
from models.payments_queue import PaymentQueue


class PaymentController:
    payments_queue: PaymentQueue
    
    async def insert_on_queue(self, payment: Payment):
        inserted = await self.payments_queue._insert_on_queue(payment)
        
        if not inserted:
            raise

        return inserted

