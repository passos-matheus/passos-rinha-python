import asyncio
import json
from typing import Dict, Any, Tuple, Coroutine

from utils.redis_client import redis_client
from payment_processor import (
    get_payment_processor_default_health_status,
    get_payment_processor_fallback_health_status
)


MAX_DEFAULT_LATENCY_MS = 100
MIN_TIMEOUT_SEC = 0.1
MAX_TIMEOUT_SEC = 10.0
LATENCY_TOLERANCE_FACTOR = 1.2

async def check_health_routine() -> Coroutine:
    while True:
        try:
            default, fallback = await asyncio.gather(
                get_payment_processor_default_health_status(),
                get_payment_processor_fallback_health_status()
            )

            best_processor, best_timeout  = select_best_processor(default, fallback)

            key = "health_processors"
            status = {
                "default": default,
                "fallback": fallback,
                "best": best_processor,
                "timeout": best_timeout
            }

            await redis_client.set(key, json.dumps(status))
            await asyncio.sleep(5)
        except Exception as e:
            print(e)
            await asyncio.sleep(5)

def calculate_timeout(latency_ms: int) -> float:
    timeout_sec = latency_ms / 1000.0
    return max(MIN_TIMEOUT_SEC, min(timeout_sec, MAX_TIMEOUT_SEC))

def is_latency_acceptable(default_ms: int, fallback_ms: int) -> bool:
    return (default_ms <= MAX_DEFAULT_LATENCY_MS or
            default_ms <= fallback_ms * LATENCY_TOLERANCE_FACTOR)

def select_best_processor(default: Dict[str, Any], fallback: Dict[str, Any]) -> Tuple[int, float]:
    default_latency_ms = int(default.get('minResponseTime', 0))
    fallback_latency_ms = int(fallback.get('minResponseTime', 0))

    default_failing = bool(default.get('failing', False))
    fallback_failing = bool(fallback.get('failing', False))

    latency_acceptable = is_latency_acceptable(default_latency_ms, fallback_latency_ms)

    if not default_failing and latency_acceptable:
        return 1, calculate_timeout(default_latency_ms)

    if not default_failing and not latency_acceptable:
        if not fallback_failing:
            return 2, calculate_timeout(fallback_latency_ms)

        return 1, calculate_timeout(default_latency_ms)

    if default_failing:
        if not fallback_failing:
            return 2, calculate_timeout(fallback_latency_ms)

    return 1, MIN_TIMEOUT_SEC
