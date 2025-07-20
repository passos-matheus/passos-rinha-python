import json
from typing import Tuple, Optional
from datetime import datetime
from redis.asyncio import Redis

from rinha.models.models import PaymentsSummary
from rinha.models.models import PaymentDatabase, PaymentProcessorSummary
from rinha.models.models import Payment, PaymentProcessorStatus, PaymentRequest


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
            'p1': PaymentProcessorStatus(payment_type='p1', minResponseTime=0, failing=False),
            'p2': PaymentProcessorStatus(payment_type='p1', minResponseTime=0, failing=False),
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

           await self.redis_client.set(key, value)
           print(f'payment procesado e salvo no banco. key: {key}, value: {value}')

           time_stamp = int(datetime.fromisoformat(payment.requestedAt).timestamp())
           await self.redis_client.zadd("payments:history", {key: time_stamp})
           print(f'timestamp do payment procesado e salvo no banco. key: {key}, value: {value}, time: {time_stamp}')
       except Exception as e:
           raise

    async def get_payments_summary(self, from_str: Optional[str], to_str: Optional[str]):
        from_ts = int(datetime.fromisoformat(from_str).timestamp()) if from_str else "-inf"
        to_ts = int(datetime.fromisoformat(to_str).timestamp()) if to_str else "+inf"

        ids = await self.redis_client.zrangebyscore("payments:history", from_ts, to_ts)

        raw = await self.redis_client.mget(ids)
        all_payments = [json.loads(p) for p in raw if p]

        default_payments = [p for p in all_payments if p.get('processor_type') == 'p1']
        fallback_payments = [p for p in all_payments if p.get('processor_type') == 'p2']

        print(default_payments)
        print(fallback_payments)

        total_d = 0
        for pd in default_payments:
            total_d = total_d + pd.get('amount')

        default = PaymentProcessorSummary(
            totalAmount=total_d,
            totalRequests=len(default_payments)
        )

        total_f = 0
        for pd in fallback_payments:
            total_f = total_f + pd.get('amount')
        fallback = PaymentProcessorSummary(
            totalAmount=total_f,
            totalRequests=len(fallback_payments)
        )

        summary = PaymentsSummary(
            default=default,
            fallback=fallback
        )
        return summary

    async def get_payment(self, payment_id: str) -> Optional[PaymentRequest]:
        try:
            response = await self.redis_client.get(payment_id)
            if not response:
                return None

            payment = json.loads(response)
            print(f'pagamento encontrado no banco! payment: {payment}')
            return PaymentRequest(**payment)
        except Exception as e:
            raise
