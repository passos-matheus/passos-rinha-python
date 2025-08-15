import os
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from utils.redis_client import redis_client
from TCP.tcp_payment_queue_client import TCPQueueClient
from intern_queue import run_workers
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("payments.setup")
shutdown_event = asyncio.Event()


@asynccontextmanager
async def lifespan(app) -> AsyncGenerator[None, None]:
    try:
        batch_process_queue = asyncio.Queue(maxsize=50000)

        await redis_client.initialize()
        await redis_client.ping()

        tcp_client = TCPQueueClient(
            host=os.getenv('TCP_QUEUE_HOST', 'worker-3'),
            port=int(os.getenv('TCP_QUEUE_PORT', '8888')),
            timeout=float(os.getenv('TCP_QUEUE_TIMEOUT', '5.0'))
        )
        app.state.queue = batch_process_queue

        try:
            stats = await tcp_client.get_stats()
            if stats['status'] == 'success':
                logger.info(f"conectado no worker via socket: {stats}")
            else:
                logger.warning(f"erro no socket: {stats}")
        except Exception as e:
            logger.error(f"Erro pra conectar no socket: {e}")

        asyncio.create_task(run_workers(tcp_client, batch_process_queue))

        await asyncio.sleep(0.15)
        yield

    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise e
    finally:
        try:
            shutdown_event.set()
            await redis_client.close()

            pending_tasks = [
                task for task in asyncio.all_tasks()
                if not task.done() and task != asyncio.current_task()
            ]

            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Shutdown error: {e}")
            raise e
