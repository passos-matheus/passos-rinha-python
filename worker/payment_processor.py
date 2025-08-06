import aiohttp

from typing import Optional
from HTTP.aiohtpp_session import get_aiohttp_session

_DEFAULT_URL: str = 'http://payment-processor-default:8080/payments'
_FALLBACK_URL: str = 'http://payment-processor-fallback:8080/payments'

_DEFAULT_HEALTH_URL: str = 'http://payment-processor-default:8080/payments/service-health'
_FALLBACK_HEALTH_URL: str = 'http://payment-processor-fallback:8080/payments/service-health'

async def process_payment_in_default_processor(payload, _timeout: Optional[float] = 1.5):
    try:
        session = await get_aiohttp_session()
        async with session.post(
                _DEFAULT_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=_timeout)
        ) as response:
            if response.status == 422:
                return "ignored"

            if response.status != 200:
                response.raise_for_status()

        return payload
    except Exception as e:
        raise e

async def process_payment_in_fallback_processor(payload, _timeout: Optional[float] = 1.5):
    try:
        session = await get_aiohttp_session()
        async with session.post(
                _FALLBACK_URL,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=_timeout)
        ) as response:
            if response.status == 422:
                return "ignored"

            if response.status != 200:
                response.raise_for_status()

        return payload
    except Exception as e:
        raise e

async def get_payment_processor_default_health_status(_timeout: Optional[float] = 1.5):
    try:
        session = await get_aiohttp_session()
        async with session.get(
                _DEFAULT_HEALTH_URL,
                timeout=aiohttp.ClientTimeout(total=_timeout)
        ) as response:
            if response.status != 200:
                response.raise_for_status()

            status = await response.json()

            return status
    except Exception as e:
        raise e

async def get_payment_processor_fallback_health_status(_timeout: Optional[float] = 1.5):
    try:
        session = await get_aiohttp_session()
        async with session.get(
                _FALLBACK_HEALTH_URL,
                timeout=aiohttp.ClientTimeout(total=_timeout)
        ) as response:
            if response.status != 200:
                response.raise_for_status()

            status = await response.json()

            return status
    except Exception as e:
        raise e
