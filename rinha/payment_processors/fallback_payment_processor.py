from rinha.models.models import PaymentProcessor


class FallbackPaymentProcessor(PaymentProcessor):
    

    async def execute(self):
        pass