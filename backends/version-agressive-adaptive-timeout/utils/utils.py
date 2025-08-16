import re
import math
import orjson
import logging
import calendar

from utils.redis_client import redis_client
from typing import Optional, List, Dict, Any, Tuple


class LogFilter(logging.Filter):
    def filter(self, record):
        if hasattr(record, 'getMessage'):
            message = record.getMessage()

            no_logger_requests = [
                '"POST /payments HTTP/1.1" 201',
                '"GET /health HTTP/1.1" 200',
                '"GET /health HTTP/1.0" 200',
                "500, message='Internal Server Error', url='http://payment-processor-fallback:8080/payments'",
                "500, message='Internal Server Error', url='http://payment-processor-default:8080/payments'"
            ]

            return not any(req in message for req in no_logger_requests)
        return True

logger = logging.getLogger("payments.main")

ISO_DATE_PATTERN = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{3}))?"
)

DEFAULT_FEE = 0.05
FALLBACK_FEE = 0.15
REDIS_TIMEOUT = 0.2

def fast_parse_int(s: str) -> int:
    value = 0
    for char in s:
        if '0' <= char <= '9':
            value = value * 10 + (ord(char) - 48)
    return value

def iso_to_timestamp(date_string: str) -> Tuple[Optional[str], bool]:
    if not date_string:
        return None, False

    if len(date_string) <= 13 and date_string.isdigit():
        return date_string, True

    match = ISO_DATE_PATTERN.match(date_string)
    if not match:
        return None, False

    year = fast_parse_int(match.group(1))
    month = fast_parse_int(match.group(2))
    day = fast_parse_int(match.group(3))
    hour = fast_parse_int(match.group(4))
    minute = fast_parse_int(match.group(5))
    second = fast_parse_int(match.group(6))
    milliseconds = fast_parse_int(match.group(7)) if match.group(7) else 0

    time_tuple = (year, month, day, hour, minute, second, 0, 0, 0)
    epoch_seconds = calendar.timegm(time_tuple)
    timestamp_ms = epoch_seconds * 1000 + milliseconds

    return str(timestamp_ms), True

def round_to_cents(value: float) -> float:
    return math.floor(value * 100 + 0.5) / 100.0

async def calculate_summary(items: List[bytes], transaction_type: str) -> Dict[str, Any]:
    total_amount = 0.0
    transaction_count = 0

    for item in items:
        if not item:
            continue

        try:
            if isinstance(item, (bytes, bytearray)):
                json_string = item.decode('utf-8')
            else:
                json_string = str(item)

            transaction = orjson.loads(json_string)
            amount = float(transaction.get('amount', 0) or 0)

            total_amount += amount
            transaction_count += 1

        except (ValueError, TypeError, orjson.JSONDecodeError) as e:
            continue

    fee_rate = DEFAULT_FEE if transaction_type == 'default' else FALLBACK_FEE
    total_fee = total_amount * fee_rate

    return {
        'totalRequests': transaction_count,
        'totalAmount': round_to_cents(total_amount),
        'totalFee': round_to_cents(total_fee),
        'feePerTransaction': fee_rate,
    }


async def get_redis_range(key: str, min_score: str, max_score: str) -> List[bytes]:
    try:
        return await redis_client.zrangebyscore(
            key,
            min=min_score,
            max=max_score
        )
    except:
        return []

