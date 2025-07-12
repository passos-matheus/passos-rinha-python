from pydantic import BaseSettings
from redis.asyncio import Redis
 

import asyncio


class Worker(BaseSettings):

    payments_queue: Redis
    principal_processor: Redis
    fallback_processor: Redis


    async def worker(self):
        while True:
            _task_ = None

            principal, fallback = await self.check_payments_health()    

            if not principal.health:
                _task_ = await self._redis_client.rpopmax()
            else:
                _task_ = await self._redis_client.rpopmin()

            try:
                processed_payment = await self.principal_processor.execute(_task_)
                await self._redis_client.save_payment()   
            except:
                pass

    
            pass


    async def check_payments_health(self):
        
        principal_status = await self._redis_client.get("principal")
        fallback_status = await self._redis_client.get("fallback")
        
        


        pass
