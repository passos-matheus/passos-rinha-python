import asyncio

from rinha.queue.redis_queue import RedisQueue
from rinha.models.models import PaymentProcessor
from rinha.workers.payment_worker import PaymentWorker, WorkerConfig
from rinha.payment_processors.principal_payment_processor import PrincipalPaymentProcessor
from rinha.payment_processors.fallback_payment_processor import FallbackPaymentProcessor


main_processor: PaymentProcessor = PrincipalPaymentProcessor()
fallback_processor: PaymentProcessor = FallbackPaymentProcessor()
queue = RedisQueue()
payment_worker_config: WorkerConfig = WorkerConfig()

payment_worker: PaymentWorker = PaymentWorker(
    cfg=payment_worker_config,
    main=main_processor,
    fallback=fallback_processor,
    queue=queue
)
 

if __name__ == 'main':

    worker_coro = payment_worker.run()
    loop = asyncio.get_running_loop()
    if not loop:
        loop = asyncio.get_event_loop()

    loop.create_task(worker_coro)
