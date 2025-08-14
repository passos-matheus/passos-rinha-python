FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY payment_worker/requirements.txt .
RUN pip install -r requirements.txt

COPY utils/ ./utils/
COPY payment_worker/ ./payment_worker/

CMD ["python", "payment_worker/payment_worker.py"]