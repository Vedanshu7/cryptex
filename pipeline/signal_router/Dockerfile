FROM python:3.11-slim AS builder
WORKDIR /build
COPY signal_router/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim
WORKDIR /app

RUN useradd -r -u 1001 -g root trading
COPY --from=builder /install /usr/local
COPY shared /app/shared
COPY signal_router/src /app/src

USER trading
ENV PYTHONPATH=/app
CMD ["python", "-m", "src.router"]
