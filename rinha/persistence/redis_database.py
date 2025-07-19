import json
from typing import Tuple, Optional

from redis.asyncio import Redis

from rinha.models.models import PaymentDatabase
from models.models import Payment, PaymentProcessorStatus, PaymentRequest


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

        print(f'retorno do redis: {status}')
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

    async def save_payment(self, payment: PaymentRequest, response):
       try:
            key = payment.correlationId
            value = json.dumps(payment.model_dump())
            print(f'salvando payment procesado no banco. key: {key}, value: {value}')
            await self.redis_client.set(key, value)
       except Exception as e:
           raise

    async def get_payments_summary(self):
        pass


    async def get_payment(self):
        pass


# /home/passos/projects/passos-rinha-python/venv/bin/python /home/passos/projects/passos-rinha-python/health_worker.py
# <redis.asyncio.client.Redis(<redis.asyncio.connection.ConnectionPool(<redis.asyncio.connection.Connection(host=localhost,port=6379,db=0)>)>)>
# True
# retorno do redis: None
# <Response [200 OK]>
# (PaymentProcessorStatus(failing=False, minResponseTime=0, payment_type='p1'), 3.344664346769511)
# (PaymentProcessorStatus(failing=True, minResponseTime=0, payment_type='p2'), 2)
# (PaymentProcessorStatus(failing=False, minResponseTime=0, payment_type='p1'), 3.344664346769511)
# (PaymentProcessorStatus(failing=True, minResponseTime=0, payment_type='p2'), 2)
# retorno do redis: {"p1": {"failing": false, "minResponseTime": 0, "payment_type": "p1"}, "p2": {"failing": true, "minResponseTime": 0, "payment_type": "p2"}}
#
# Erro em check_health: 'ReadTimeout' object has no attribute 'headers'
# Erro no loop geral: cannot unpack non-iterable NoneType object
