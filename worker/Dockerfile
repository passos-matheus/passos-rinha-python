FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY worker/ .

RUN pip install -r requirements.txt

CMD ["python", "payment_worker.py"]
