---
title: Cryptex
type: Distributed System
projectURL: cryptex
descriptionShort: A multi-tenant crypto algorithmic trading platform — Kafka event pipeline feeding OMS/EMS microservices, with full observability.
descriptionLong: Cryptex is a multi-tenant crypto algorithmic trading platform built as an event-driven pipeline. Binance WebSocket ticks arrive at 100ms intervals and flow through a Python exchange connector into Kafka, where a candle aggregator rolls them into 5-minute OHLCV bars. A signal pipeline fans those candles out to two independent sources — a LightGBM model and an LLM-backed source running Claude or GPT — and a per-tenant signal router merges their output into order requests. Downstream, OMS and EMS microservices written in C# and .NET 8 validate and execute orders against the Binance REST testnet, backed by PostgreSQL with TimescaleDB for time-series storage. The whole system is instrumented end to end with Prometheus metrics, Grafana dashboards, and Jaeger distributed tracing.
viewCodeUrl: https://github.com/Vedanshu7/cryptex
viewProjectUrl:
projectImg:
technologies:
  - Python
  - C#
  - .NET
  - Kafka
  - PostgreSQL
  - Prometheus
  - Grafana
  - Docker
---
