import asyncio
import json
import random

from redis.asyncio import Redis
from typing import Optional
from httpx import AsyncClient, Response, Headers

from httpx import HTTPStatusError

from models.models import PaymentDatabase, PaymentProcessorStatus


class HealthCheckerWorker:
    default_url: str = 'http://localhost:8001/payments/service-health'
    fallback_url: str = 'http://localhost:8002/payments/service-health'

    def __init__(self, async_client: Optional[AsyncClient], db: PaymentDatabase):
        self.async_client = async_client if async_client else AsyncClient()
        self.db = db


    async def run(self):
        while True:
            try:
                results = await self.check_health()
                delays: list[int] = []
                for result in results:
                    if isinstance(result, Exception):
                        print(f"Erro em uma das chamadas: {result}")
                        await asyncio.sleep(5)
                        break

                    status, delay = result
                    print(result)
                    delays.append(delay)

                max_ = max(delays)
                await asyncio.sleep(max_)
            except Exception as e:
                print(f"Erro no loop geral: {e}")
                await asyncio.sleep(5)

    async def check_health(self):
        try:
            redis_data = await self.db.check_health()

            if not redis_data or not isinstance(redis_data, dict):
                print("Status ainda não disponível no Redis!")

            responses = await asyncio.gather(
                self.get_payment_health_status(url=self.default_url, _type='p1'),
                self.get_payment_health_status(url=self.fallback_url, _type='p2'),
                return_exceptions=True
            )

            payments_status: list = []
            for resp in responses:
                payment_type = resp.headers.get('payment_type')

                if resp.status_code != 200:
                    actual = redis_data.get(payment_type)
                    resp = (
                        actual if actual else PaymentProcessorStatus(failing=True, minResponseTime=0, payment_type=payment_type),
                        int(resp.headers.get('retry_after', 5))
                    )
                    payments_status.append(resp)
                    continue

                status = resp.json()
                status["payment_type"] = resp.headers.get('payment_type')
                resp = (PaymentProcessorStatus(**status), 5)
                payments_status.append(resp)

            default, fallback = payments_status

            print(default)
            print(fallback)
            await self.db.save_health_status(default[0], fallback[0])
            return payments_status
        except Exception as e:
            print(f"Erro em check_health: {e}")
            return None, 5

    async def get_payment_health_status(self, url: str, _type: str):
        try:
            response = await self.async_client.get(url)
            print(response)
            headers = dict(response.headers)
            headers["payment_type"] = _type

            resp = Response(
                status_code=response.status_code,
                headers=Headers(headers),
                content=response.content,
                request=response.request,
            )

            return resp
        except Exception as e:
            print(e)
            raise e
