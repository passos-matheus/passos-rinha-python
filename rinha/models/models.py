import datetime
import logging
from typing import Tuple

from httpx import AsyncClient
from abc import ABC, abstractmethod
from uuid import UUID
from pydantic import BaseModel, PrivateAttr

class Payment(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    correlationId: UUID
    amount: float

 
class PaymentQueue(ABC, BaseModel):
    class Config:
        arbitrary_types_allowed = True
    queue_name: str

    @abstractmethod
    async def get_from_top(self):
        pass

    @abstractmethod
    async def get_from_bottom(self):
        pass

    @abstractmethod
    async def insert_on_queue(self, payment: Payment) -> bool:
        pass

    @abstractmethod
    async def has_items(self) -> bool:
        pass


class PaymentProcessorStatus(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    failing: bool
    minResponseTime: int
    payment_type: str


class PaymentRequest(BaseModel):
        correlationId: str
        amount: float
        requestedAt: str
        processor_type: str

class PaymentDatabase(BaseModel, ABC):
    class Config:
        arbitrary_types_allowed = True

    @abstractmethod
    async def get_payments_summary(self):

        pass

    @abstractmethod
    async def save_payment(self, payment: PaymentRequest, response):
        pass

    @abstractmethod
    async def get_payment(self):
        pass

    @abstractmethod
    async def save_health_status(self, p1: PaymentProcessorStatus, p2: PaymentProcessorStatus):
        pass

    @abstractmethod
    async def check_health(self) -> dict[str, PaymentProcessorStatus]:


        pass


class PaymentProcessor(ABC, BaseModel):
    class Config:
        arbitrary_types_allowed = True

    endpoint: str
    db: PaymentDatabase
    async_client: AsyncClient
    _logger: logging.Logger = PrivateAttr(default_factory=lambda: logging.getLogger())

    @abstractmethod
    async def execute(self, payment: Payment):
        pass



class PaymentProcessorSummary(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    totalRequests: int
    totalAmount: float


class PaymentsSummary(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    default: PaymentProcessorSummary
    fallback: PaymentProcessorSummary