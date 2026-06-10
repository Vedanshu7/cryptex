FROM python:3.11-slim AS builder
WORKDIR /build
COPY candle_aggregator/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim
WORKDIR /app

RUN useradd -r -u 1001 -g root trading
COPY --from=builder /install /usr/local
COPY shared /app/shared
COPY candle_aggregator/src /app/src

USER trading
ENV PYTHONPATH=/app
CMD ["python", "-m", "src.aggregator"]
