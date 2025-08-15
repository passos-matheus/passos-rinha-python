import os
import uvicorn
import asyncio
import logging

from TCP.tcp_payment_queue_client import TCPQueueClient
from setup import lifespan
from starlette.responses import Response
from utils.redis_client import redis_client
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from utils.utils import (
    logger,
    get_redis_range,
    calculate_summary,
    LogFilter,
    iso_to_timestamp,
    REDIS_TIMEOUT
)


app = FastAPI(lifespan=lifespan)

RESPONSE_MESSAGES = {
    "accepted": {"status": "accepted"},
    "purged": {"status": "purged"},
    "empty_body": {"error": "empty body"},
    "queue_full": {"error": "queue full"},
    "tcp_error": {"error": "worker unavailable"}
}

@app.post('/payments', status_code=201)
async def create_payment(request: Request) -> Response:
    try:
        body = await request.body()

        if not body:
            raise HTTPException(
                status_code=400,
                detail=RESPONSE_MESSAGES["empty_body"]
            )

        tcp_client: TCPQueueClient = request.app.state.tcp_queue_client
        await tcp_client.send_payment(body)
        return Response(status_code=201)
    except Exception as e:
        return Response(status_code=500)

@app.get('/payments-summary')
async def get_payments_summary(request: Request) -> JSONResponse:
    try:
        from_ = request.query_params.get('from')
        to = request.query_params.get('to')

        min_score = '-inf'
        max_score = '+inf'

        if from_:
            timestamp, success = iso_to_timestamp(from_)
            if success and timestamp is not None:
                min_score = timestamp

        if to:
            timestamp, success = iso_to_timestamp(to)
            if success and timestamp is not None:
                max_score = timestamp

        default_task = asyncio.create_task(
            get_redis_range('default', min_score, max_score)
        )
        fallback_task = asyncio.create_task(
            get_redis_range('fallback', min_score, max_score)
        )

        try:
            default_items, fallback_items = await asyncio.wait_for(
                asyncio.gather(default_task, fallback_task),
                timeout=REDIS_TIMEOUT
            )
        except asyncio.TimeoutError:
            raise
    except:
        default_items, fallback_items = [], []

    default_summary = await calculate_summary(default_items, 'default')
    fallback_summary = await calculate_summary(fallback_items, 'fallback')

    return JSONResponse(
        status_code=200,
        content={
            'default': default_summary,
            'fallback': fallback_summary,
        }
    )

@app.post('/purge-payments')
async def purge_payments() -> JSONResponse:
    try:
        await redis_client.delete('default', 'fallback')

        return JSONResponse(
            status_code=200,
            content=RESPONSE_MESSAGES["purged"]
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": f"Failed to purge data: {str(e)}"}
        )

@app.get('/health')
async def health_check() -> PlainTextResponse:
    return PlainTextResponse('OK\n')

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.CRITICAL)
    logging.disable(logging.INFO)

    port = int(os.getenv('PORT', '8080'))
    host = '0.0.0.0'

    uvicorn.run(
        'payment_api:app',
        host=host,
        port=port,
        reload=False,
        log_level="critical",
        access_log=False,
        use_colors=False,
        loop="uvloop",
        http="httptools",
        backlog=2048,
    )
