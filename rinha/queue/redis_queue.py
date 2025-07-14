from rinha.models.models import PaymentQueue
from rinha.models.models import Payment
from redis.asyncio import Redis

import json

class RedisQueue(PaymentQueue):
    redis_client: Redis

    async def get_from_top(self):
        result = await self.redis_client.zpopmax(self.queue_name)
        
        if result:
            member, _ = result[0]
            return member
        
        return None

    async def get_from_bottom(self):
        result = await self.redis_client.zpopmin(self.queue_name)
        if result:
            member, _ = result[0]
            return member
        
        return None

    async def _insert_on_queue(self, payment: Payment):
        try:
            score: float = payment.amount
            member = json.dumps(payment)

            await self.redis_client.zadd(self.queue_name, {member: score})
        except Exception as e:
            raise RuntimeError("Erro, ajustar dps") from e

