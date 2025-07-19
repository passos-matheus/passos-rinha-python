import json
from typing import Tuple, Optional

from redis.asyncio import Redis

from rinha.models.models import PaymentDatabase
from models.models import Payment, PaymentProcessorStatus

class RedisDatabase(PaymentDatabase):
    redis_client: Redis


    async def save_health_status(self, p1: PaymentProcessorStatus, p2: PaymentProcessorStatus):
        key = 'payments-health'
        value = json.dumps({
            "p1": p1.model_dump(),
            "p2": p2.model_dump()
        })

        await self.redis_client.set(key, value)
        return True

    async def check_health(self) -> dict[str, PaymentProcessorStatus]:
        status = await self.redis_client.get('payments-health')

        if not status:
            return {
            'p1': None,
            'p2': None
        }

        data = json.loads(status)

        p1 = PaymentProcessorStatus.model_validate(data['p1'])
        p2 = PaymentProcessorStatus.model_validate(data['p2'])
        response = {
            'p1': p1,
            'p2': p2
        }

        return response

    async def save_payment(self, payment: Payment, response):
        pass

    async def get_payments_summary(self):
        pass


    async def get_payment(self):
        pass
