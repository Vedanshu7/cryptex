FROM python:3.11-slim AS builder
WORKDIR /build
COPY signal_pipeline/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim
WORKDIR /app

# libgomp1 — OpenMP runtime lightgbm's compiled extension links against;
# python:3.11-slim doesn't include it, so importing lightgbm fails at runtime
# (build succeeds since it's only needed to load the .so, not to pip install).
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -u 1001 -g root trading
COPY --from=builder /install /usr/local
COPY shared /app/shared
COPY signal_pipeline/src /app/src

USER trading
ENV PYTHONPATH=/app
CMD ["python", "-m", "src.runner"]
