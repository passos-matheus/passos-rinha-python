FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY api/requirements.txt .
RUN pip install -r requirements.txt

COPY utils/ ./utils/
COPY api/ ./api/

CMD ["python", "api/payment_api.py"]