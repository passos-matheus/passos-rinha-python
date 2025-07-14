from rinha.models.models import PaymentProcessor, Payment
import datetime

class PrincipalPaymentProcessor(PaymentProcessor):


    async def execute(self, payment: Payment):
        try: 

            payment_request = payment.model_dump()
            payment_request["requestedAt"] = datetime.datetime.now()
            
            endpoint = f"{self.base_url}/aaa"
            resp = await self.client.post(endpoint, json=payment_request)
            if resp.status_code != 200:
                raise

            return resp
        except Exception as e:
            raise e

