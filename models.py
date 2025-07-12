import logging

from abc import ABC, abstractmethod
from uuid import UUID
from pydantic import BaseSettings, BaseModel PrivateAtribute
from fastapi import Decimal

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


class PaymentProcessor(ABC, BaseSettings):
    max_retries: int = 2
    _logger: logging.Logger = PrivateAtribute(default_factory=lambda: logging.getLogger())

    @abstractmethod
    async def execute(self):
        pass

    @abstractmethod
    async def check_health(self)
        pass


class PaymentProcessorSummary(BaseModel):
    totalRequests: int
    totalAmount: Decimal


class PaymentsSummary(BaseModel):
    default: PaymentProcessorSummary
    fallback: PaymentProcessorSummary


class PaymentProcessorStatus(BaseModel):
    failing: bool
    minResponseTime: int


class PaymentDatabase(BaseSettings, ABC):


    async def get_payments_summary():    
    
        pass