from datetime import datetime

from models.models import PaymentProcessorStatus, PaymentRequest
from rinha.models.models import Payment, PaymentProcessor
from pydantic import Field
from httpx import AsyncClient



class FallbackPaymentProcessor(PaymentProcessor):

    async def execute(self, payment: Payment):
        try:
            payment_already_exists_on_db = await self.db.get_payment(payment_id=str(payment.correlationId))

            if payment_already_exists_on_db:
                print('pagamento já processado!')
                return None

            payment_request: dict = {
                "correlationId": str(payment.correlationId),
                "amount": payment.amount,
                "requestedAt": datetime.now().isoformat()
            }

            resp = await self.async_client.post(self.endpoint, json=payment_request)
            data = resp.json()

            if resp.status_code != 200:
                resp.raise_for_status()

            payment_request['processor_type'] = 'p2'
            request_model = PaymentRequest(**payment_request)

            await self.db.save_payment(request_model, response=data)
            return resp
        except Exception as e:
            raise e

    async def check_health(self) -> PaymentProcessorStatus:
        pass




    