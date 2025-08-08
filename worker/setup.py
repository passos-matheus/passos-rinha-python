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
        logger.info("conectando ao Redis")
        await redis_client.initialize()

        await redis_client.ping()
        logger.info("conexão com Redis feita")

        logger.info("iniciando workers")
        worker_task = asyncio.create_task(
            run_workers()
        )

        await asyncio.sleep(0.15)

        logger.info("workers iniciados")
        yield

    except Exception as e:
        logger.error(f"Erro durante inicialização: {e}")
        raise e

    finally:
        logger.info("iniciando shutdown")

        try:
            shutdown_event.set()

            if worker_task and not worker_task.done():
                worker_task.cancel()

                try:
                    await asyncio.wait_for(worker_task, timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("timeout ao aguardar workers finalizarem")
                except asyncio.CancelledError:
                    logger.info("workers cancelados")

            logger.info("fechando conexão com Redis")
            await redis_client.close()

            pending_tasks = [
                task for task in asyncio.all_tasks()
                if not task.done() and task != asyncio.current_task()
            ]

            if pending_tasks:
                logger.info(f"Aguardando {len(pending_tasks)} tasks pendentes")
                await asyncio.gather(*pending_tasks, return_exceptions=True)

            logger.info("shutdown concluído com sucesso")

        except Exception as e:
            logger.error(f"Erro durante shutdown: {e}")
