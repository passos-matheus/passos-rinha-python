from datetime import datetime

from models.models import PaymentProcessorStatus, PaymentRequest
from rinha.models.models import PaymentProcessor, Payment


class PrincipalPaymentProcessor(PaymentProcessor):

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

            payment_request['processor_type'] = 'p1'
            request_model = PaymentRequest(**payment_request)

            await self.db.save_payment(request_model, response=data)
            return resp
        except Exception as e:
            raise e

