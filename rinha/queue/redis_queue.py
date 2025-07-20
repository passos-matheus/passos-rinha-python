import json
from statistics import correlation

from redis.asyncio import Redis
from rinha.models.models import Payment
from rinha.models.models import PaymentQueue


class RedisQueue(PaymentQueue):
    redis_client: Redis

    async def get_from_top(self):
        result = await self.redis_client.zpopmax(self.queue_name)
        
        if result:
            member, _ = result[0]
            payment = json.loads(member)
            return Payment(**payment)
        
        return None

    async def get_from_bottom(self):
        result = await self.redis_client.zpopmin(self.queue_name)
        if result:
            member, _ = result[0]
            payment = json.loads(member)
            return Payment(**payment)
        
        return None

    async def insert_on_queue(self, payment: Payment):
        try:
            score = float(payment.amount)
            member = json.dumps({
                "correlationId": str(payment.correlationId),
                "amount": payment.amount
            })

            await self.redis_client.zadd(self.queue_name, {member: score})
        except Exception as e:
            raise RuntimeError("Erro, ajustar dps") from e

    async def has_items(self) -> bool:
        count = await self.redis_client.zcard(self.queue_name)
        # print(f'payments na fila: {count}')
        return count > 0
