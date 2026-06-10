FROM python:3.11-slim AS builder
WORKDIR /build
# Build context is pipeline/ — prefix all paths with the service subdirectory.
COPY exchange_connector/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim
WORKDIR /app

RUN useradd -r -u 1001 -g root trading
COPY --from=builder /install /usr/local
COPY shared /app/shared
COPY exchange_connector/src /app/src

USER trading
ENV PYTHONPATH=/app
CMD ["python", "-m", "src.connector"]
