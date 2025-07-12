from models.payment_processor import PaymentProcessor


class FallbackPaymentProcessor(PaymentProcessor):


    async def execute(self):
        pass