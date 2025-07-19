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

            if not redis_data or not isinstance(redis_data, tuple) or len(redis_data) != 2:
                print("Status ainda não disponível no Redis!")

            responses = await asyncio.gather(
                self.mock_response_200(),
                self.mock_response_429(), return_exceptions=True
            )

            payments_status: list = []
            for resp in responses:
                payment_type = resp.headers.get('payment-type')

                if resp.status_code != 200:
                    actual = redis_data.get(payment_type)

                    resp = (
                        actual if actual else PaymentProcessorStatus(failing=False, minResponseTime=0, payment_type=payment_type),
                        int(resp.headers.get('retry_after', random.uniform(2, 8)))
                    )

                    print(resp)
                    payments_status.append(resp)
                    continue

                status = resp.json()
                status["payment_type"] = resp.headers.get('payment-type', 'aaaaaaaaaa')
                resp = (PaymentProcessorStatus(**status), random.uniform(2, 8))
                payments_status.append(resp)

            default, fallback = payments_status


            print(default)
            print(fallback)
            # await self.db.save_health_status(default, fallback)
            return payments_status
            #
            # actual_p1, _ = redis_data
            #
            # response = await self.async_client.get(url=url)
            #
            # if response.status_code == 429:
            #     retry_time = int(response.headers.get("Retry-After", "5"))
            #     return actual_p1, retry_time
            #
            # response.raise_for_status()
            #
            # data = response.json()
            #
            # return data, 5

        except Exception as e:
            print(f"Erro em check_health: {e}")
            return None, 5

    async def mock_response_429(self):
        await asyncio.sleep(0.1)

        return Response(
            status_code=429,
            headers=Headers({"Retry-After": "5", "payment-type": "p2"}),
            content=b"Too many requests"
        )

    async def mock_response_200(self):
        await asyncio.sleep(0.12)

        json_response = json.dumps({
            "failing": False,
            "minResponseTime": 120
        })
        return Response(
            status_code=200,
            headers=Headers({"payment-type": "p1"}),
            content=json_response
        )


