import json
import aiohttp

session = None

async def get_aiohttp_session():
    global session

    if session is None:
        timeout = aiohttp.ClientTimeout(
            total=1.8,
            connect=0.1,
            sock_read=1.2
        )

        connector = aiohttp.TCPConnector(
            limit=30,
            limit_per_host=25,
            keepalive_timeout=300,
            enable_cleanup_closed=True,
            use_dns_cache=True,
            ttl_dns_cache=600,
            force_close=False,
        )

        session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                "Connection": "keep-alive",
                "User-Agent": "payment-worker-aiohttp-final/1.0",
                "Accept": "application/json"
            },
            json_serialize=json.dumps
        )

    # TO-DO testar orm json.
    return session
