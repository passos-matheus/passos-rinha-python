import logging

from httpx import AsyncClient
from abc import ABC, abstractmethod
from uuid import UUID
from pydantic import BaseSettings, BaseModel, PrivateAttr
from fastapi import Decimal
from models.models import PaymentDatabase
class Payment(BaseModel):
    correlationId: UUID
    amount: Decimal

 
class PaymentQueue(ABC, BaseSettings):
    queue_name: str
    
    @abstractmethod
    async def get_from_top(self):
        pass

    @abstractmethod
    async def get_from_bottom(self):
        pass

    @abstractmethod
    async def _insert_on_queue(self, payment: Payment) -> bool:
        pass


class PaymentProcessorStatus(BaseModel):
    failing: bool
    minResponseTime: int


class PaymentProcessor(ABC, BaseSettings):
    db: PaymentDatabase
    async_client: AsyncClient
    _logger: logging.Logger = PrivateAttr(default_factory=lambda: logging.getLogger())

    @abstractmethod
    async def execute(self):
        pass

    @abstractmethod
    async def check_health(self) -> PaymentProcessorStatus:
        pass


class PaymentProcessorSummary(BaseModel):
    totalRequests: int
    totalAmount: Decimal


class PaymentsSummary(BaseModel):
    default: PaymentProcessorSummary
    fallback: PaymentProcessorSummary


class PaymentDatabase(BaseSettings, ABC):
    async_clent: AsyncClient
    
    @abstractmethod
    async def get_payments_summary():    
    
        pass

    @abstractmethod
    async def save_payment():
        pass
    
    @abstractmethod
    async def get_payment():
        pass