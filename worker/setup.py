import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from payment_worker import run_workers
from utils.redis_client import redis_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("payments.setup")

worker_task: Optional[asyncio.Task] = None
shutdown_event = asyncio.Event()

@asynccontextmanager
async def lifespan(app) -> AsyncGenerator[None, None]:
    global worker_task

    try:
        await redis_client.initialize()
        await redis_client.ping()

        worker_task = asyncio.create_task(
            run_workers()
        )

        await asyncio.sleep(0.15)
        yield
    except Exception as e:
        raise e
    finally:

        try:
            shutdown_event.set()

            if worker_task and not worker_task.done():
                worker_task.cancel()

                try:
                    await asyncio.wait_for(worker_task, timeout=5.0)
                except asyncio.TimeoutError:
                    raise
                except asyncio.CancelledError:
                    raise

            await redis_client.close()

            pending_tasks = [
                task for task in asyncio.all_tasks()
                if not task.done() and task != asyncio.current_task()
            ]

            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
        except Exception as e:
            raise e
