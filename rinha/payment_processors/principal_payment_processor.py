import datetime

from models.models import PaymentProcessorStatus
from rinha.models.models import PaymentProcessor, Payment


class PrincipalPaymentProcessor(PaymentProcessor):


    async def execute(self, payment: Payment):
        try:
            payment_request = payment.model_dump()
            payment_request["requestedAt"] = datetime.datetime.now()

            endpoint = f"{self.base_url}/payments"
            resp = await self.async_client.post(endpoint, json=payment_request)
            if resp.status_code != 200:
                raise

            return resp
        except Exception as e:
            raise e

