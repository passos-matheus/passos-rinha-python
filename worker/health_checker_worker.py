from httpx import AsyncClient
from pydantic import BaseSettings


class HealthCheckerWorker(BaseSettings):
    cache_engine: str
    async_client: AsyncClient = AsyncClient
 
    payment_base_url: str

    async def health_check_worker(self):
        while True:
            
            try:
                
                self.async_client.get()
                
                key: str = 'principalhealth'
                value: str = ''
            except:
                pass
            
            pass
        pass
