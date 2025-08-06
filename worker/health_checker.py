import asyncio

from payment_worker import redis_client
from payment_processor import (
    get_payment_processor_default_health_status,
    get_payment_processor_fallback_health_status
)

async def check_health_routine():
    while True:
        try:
            default, fallback = await asyncio.gather(
                get_payment_processor_fallback_health_status(),
                get_payment_processor_default_health_status()
            )

            key = "health_processors"
            status = {
                "default": default,
                "fallback": fallback
            }

            await redis_client.set(key, status)
            await asyncio.sleep(5)
        except Exception as e:
            print(e)
            await asyncio.sleep(5)
            continue
