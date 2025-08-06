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

            best = set_best_processor(default, fallback)

            key = "health_processors"
            status = {
                "default": default,
                "fallback": fallback,
                "best": best
            }

            await redis_client.set(key, status)
            await asyncio.sleep(5)
        except Exception as e:
            print(e)
            await asyncio.sleep(5)
            continue

def set_best_processor(default, fallback):
        main_ok = not default['failing']
        latency_ok = default['minResponseTime'] <= fallback['minResponseTime'] * 1.2

        if main_ok and latency_ok:
            return 1

        if main_ok and not latency_ok:
            return 2

        if not main_ok and latency_ok:
            return 1

        return 2
