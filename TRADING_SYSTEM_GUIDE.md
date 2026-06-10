# Crypto Algorithmic Trading Platform — System Build Guide

> This document is a comprehensive guide for Claude to scaffold, design, and build a production-grade crypto algorithmic trading platform locally. Follow every section in order. Do not skip sections.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Repository Structure](#2-repository-structure)
3. [Code Standards — Python](#3-code-standards--python)
4. [Code Standards — C#](#4-code-standards--c)
5. [Component 1 — Exchange Connector (Python)](#5-component-1--exchange-connector-python)
6. [Component 2 — Candle Aggregator (Python)](#6-component-2--candle-aggregator-python)
7. [Component 3 — ML Signal Pipeline (Python + Airflow)](#7-component-3--ml-signal-pipeline-python--airflow)
8. [Component 4 — Signal Router (Python)](#8-component-4--signal-router-python)
9. [Component 5 — OMS Microservice (C# / .NET 8)](#9-component-5--oms-microservice-c--net-8)
10. [Component 6 — EMS Microservice (C# / .NET 8)](#10-component-6--ems-microservice-c--net-8)
11. [Component 7 — Kafka Setup](#11-component-7--kafka-setup)
12. [Component 8 — Observability](#12-component-8--observability)
13. [Docker Compose — Full Local Stack](#13-docker-compose--full-local-stack)
14. [Database Schemas](#14-database-schemas)
15. [Environment Variables](#15-environment-variables)

---

## 1. System Overview

### Architecture

```
Binance WebSocket (100ms ticks)
        ↓
[Python] Exchange Connector
        ↓ Kafka: market-data-raw
[Python] Candle Aggregator (5min OHLCV)
        ↓ Kafka: market-data-candles
[Python] ML Signal Pipeline (Airflow DAG)
        ↓ Kafka: trade-signals
[Python] Signal Router (per-tenant routing)
        ↓ Kafka: order-requests
[C# .NET 8] OMS — risk checks, order lifecycle
        ↓ Kafka: validated-orders
[C# .NET 8] EMS — HMAC signing, Binance execution
        ↓ Kafka: order-fills
[C# .NET 8] OMS — update positions, P&L
        ↓ WebSocket
[React] Dashboard
```

### Kafka Topics

| Topic | Producer | Consumer |
|---|---|---|
| market-data-raw | Exchange Connector | Candle Aggregator |
| market-data-candles | Candle Aggregator | ML Pipeline |
| trade-signals | ML Pipeline | Signal Router |
| order-requests | Signal Router | OMS |
| validated-orders | OMS | EMS |
| order-fills | EMS | OMS |

---

## 2. Repository Structure

```
trading-platform/
│
├── python/
│   ├── exchange_connector/
│   │   ├── src/
│   │   │   ├── __init__.py
│   │   │   ├── connector.py
│   │   │   ├── normalizer.py
│   │   │   └── producer.py
│   │   ├── tests/
│   │   │   ├── __init__.py
│   │   │   └── test_connector.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── candle_aggregator/
│   │   ├── src/
│   │   │   ├── __init__.py
│   │   │   ├── aggregator.py
│   │   │   └── models.py
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── signal_pipeline/
│   │   ├── dags/
│   │   │   └── signal_dag.py
│   │   ├── src/
│   │   │   ├── __init__.py
│   │   │   ├── model.py
│   │   │   └── features.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── signal_router/
│   │   ├── src/
│   │   │   ├── __init__.py
│   │   │   ├── router.py
│   │   │   └── tenant_config.py
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   └── shared/
│       ├── __init__.py
│       ├── kafka_client.py
│       ├── db_client.py
│       ├── models.py
│       └── logger.py
│
├── dotnet/
│   ├── TradingPlatform.sln
│   │
│   ├── src/
│   │   ├── OMS/
│   │   │   ├── TradingPlatform.OMS.Api/
│   │   │   │   ├── Controllers/
│   │   │   │   ├── Program.cs
│   │   │   │   └── appsettings.json
│   │   │   ├── TradingPlatform.OMS.Application/
│   │   │   │   ├── Commands/
│   │   │   │   ├── Queries/
│   │   │   │   └── Handlers/
│   │   │   ├── TradingPlatform.OMS.Domain/
│   │   │   │   ├── Entities/
│   │   │   │   ├── ValueObjects/
│   │   │   │   └── Interfaces/
│   │   │   └── TradingPlatform.OMS.Infrastructure/
│   │   │       ├── Repositories/
│   │   │       ├── Kafka/
│   │   │       └── Persistence/
│   │   │
│   │   └── EMS/
│   │       ├── TradingPlatform.EMS.Api/
│   │       ├── TradingPlatform.EMS.Application/
│   │       ├── TradingPlatform.EMS.Domain/
│   │       └── TradingPlatform.EMS.Infrastructure/
│   │
│   └── tests/
│       ├── TradingPlatform.OMS.UnitTests/
│       └── TradingPlatform.EMS.UnitTests/
│
├── infra/
│   ├── docker-compose.yml
│   ├── docker-compose.override.yml
│   └── kafka/
│       └── create-topics.sh
│
├── db/
│   └── migrations/
│       ├── 001_create_orders.sql
│       ├── 002_create_positions.sql
│       └── 003_create_tenants.sql
│
└── .env.example
```

---

## 3. Code Standards — Python

### 3.1 General Rules

```
- All functions must have type hints
- All public functions must have docstrings
- Max function length: 30 lines
- Single responsibility — one function does one thing
- DRY — never duplicate logic, extract to shared/
- All errors must be caught and logged — never silently swallowed
- Use dataclasses or Pydantic models for data structures
- No magic strings — use constants or enums
- Comments explain WHY not WHAT
- Comments end with periods.
```

### 3.2 File Structure

```python
# Every Python file follows this order:
# 1. Standard library imports
# 2. Third party imports
# 3. Local imports
# 4. Constants
# 5. Models / dataclasses
# 6. Classes
# 7. Functions
# 8. Main entry point

import json                          # 1. stdlib
import logging
from dataclasses import dataclass
from datetime import datetime

from confluent_kafka import Producer  # 2. third party
from pydantic import BaseModel

from shared.logger import get_logger  # 3. local
from shared.models import Tick

KAFKA_TOPIC = "market-data-raw"       # 4. constants
DEFAULT_TIMEOUT_MS = 5000
```

### 3.3 Error Handling

```python
# WRONG — silently swallowed
def process_tick(tick: dict) -> None:
    try:
        publish_to_kafka(tick)
    except Exception:
        pass

# WRONG — too broad, no context
def process_tick(tick: dict) -> None:
    try:
        publish_to_kafka(tick)
    except Exception as e:
        print(e)

# CORRECT — specific, logged, re-raised if critical
def process_tick(tick: Tick) -> None:
    """Process a single market tick and publish to Kafka.
    
    Args:
        tick: Normalized tick data from exchange.
        
    Raises:
        KafkaPublishError: When message cannot be delivered after retries.
    """
    logger = get_logger(__name__)
    
    try:
        publish_to_kafka(tick)
    except KafkaException as e:
        logger.error(
            "Failed to publish tick to Kafka.",
            extra={
                "symbol": tick.symbol,
                "error": str(e),
                "topic": KAFKA_TOPIC,
            }
        )
        raise KafkaPublishError(
            f"Kafka publish failed for {tick.symbol}."
        ) from e
```

### 3.4 DRY — Extract shared logic

```python
# WRONG — duplicated Kafka config in every service
class ExchangeConnector:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": "kafka:9092",
            "client.id": "exchange-connector",
        })

class CandleAggregator:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": "kafka:9092",  # duplicated
            "client.id": "candle-aggregator",
        })

# CORRECT — shared Kafka client in shared/kafka_client.py
# shared/kafka_client.py
class KafkaClientFactory:
    """Factory for creating configured Kafka producers and consumers."""

    @staticmethod
    def create_producer(client_id: str) -> Producer:
        """Create a Kafka producer with standard config.
        
        Args:
            client_id: Unique identifier for this producer instance.
        """
        config = {
            "bootstrap.servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
            "client.id": client_id,
            "acks": "all",
            "retries": 3,
        }
        return Producer(config)

    @staticmethod
    def create_consumer(group_id: str, topics: list[str]) -> Consumer:
        """Create a Kafka consumer subscribed to given topics.
        
        Args:
            group_id: Consumer group identifier.
            topics: List of topic names to subscribe to.
        """
        config = {
            "bootstrap.servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
            "group.id": group_id,
            "auto.offset.reset": "latest",
        }
        consumer = Consumer(config)
        consumer.subscribe(topics)
        return consumer
```

### 3.5 Models with Pydantic

```python
# shared/models.py
from pydantic import BaseModel, field_validator
from datetime import datetime
from enum import Enum

class TradeSide(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"

class Tick(BaseModel):
    symbol:    str
    price:     float
    volume:    float
    timestamp: datetime

    @field_validator("price", "volume")
    @classmethod
    def must_be_positive(cls, value: float) -> float:
        """Ensure price and volume are always positive."""
        if value <= 0:
            raise ValueError("Price and volume must be positive.")
        return value

class Candle(BaseModel):
    symbol:     str
    open:       float
    high:       float
    low:        float
    close:      float
    volume:     float
    opened_at:  datetime
    closed_at:  datetime
    timeframe:  str = "5m"

class TradeSignal(BaseModel):
    id:           str
    symbol:       str
    side:         TradeSide
    confidence:   float
    generated_at: datetime
    expires_at:   datetime
```

### 3.6 Logging standard

```python
# shared/logger.py
import logging
import json
import os

def get_logger(name: str) -> logging.Logger:
    """Get a structured JSON logger for the given module name."""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(
            os.getenv("LOG_LEVEL", "INFO")
        )
    
    return logger

class JsonFormatter(logging.Formatter):
    """Formats log records as JSON for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record),
            "level":     record.levelname,
            "service":   record.name,
            "message":   record.getMessage(),
        }
        if hasattr(record, "extra"):
            log_data.update(record.__dict__.get("extra", {}))
        return json.dumps(log_data)
```

---

## 4. Code Standards — C#

### 4.1 General Rules

```
- Every class in its own file.
- Interfaces prefix with I — IOrderRepository, IRiskService.
- Use async/await everywhere — no blocking calls.
- Never use var for complex types — be explicit.
- All public methods must have XML doc comments.
- DRY — extract shared logic to base classes or extensions.
- Never catch Exception broadly — catch specific exceptions.
- Use records for immutable data (signals, events).
- Use sealed classes where inheritance is not intended.
- Comments explain WHY not WHAT.
- Comments end with periods.
```

### 4.2 Project Layer Rules

```
Domain Layer:
  - No dependencies on other layers.
  - Contains entities, value objects, interfaces.
  - Pure C# — no NuGet packages except primitives.

Application Layer:
  - Depends on Domain only.
  - Contains commands, queries, handlers (CQRS via MediatR).
  - No direct database or Kafka calls.

Infrastructure Layer:
  - Depends on Application and Domain.
  - Implements interfaces defined in Domain.
  - Contains EF Core, Kafka, external APIs.

API Layer:
  - Depends on Application only.
  - Contains controllers, middleware, DI registration.
  - No business logic here.
```

### 4.3 DDD — Domain Entities

```csharp
// Domain/Entities/Order.cs
namespace TradingPlatform.OMS.Domain.Entities;

/// <summary>
/// Aggregate root representing a single trade order.
/// Encapsulates all state transitions for order lifecycle.
/// </summary>
public sealed class Order
{
    public Guid    Id        { get; private set; }
    public Guid    TenantId  { get; private set; }
    public string  Symbol    { get; private set; }
    public string  Side      { get; private set; }
    public decimal Quantity  { get; private set; }
    public decimal Price     { get; private set; }
    public OrderStatus Status { get; private set; }
    public DateTime CreatedAt { get; private set; }
    public DateTime? FilledAt { get; private set; }

    // Private constructor — use factory method.
    private Order() { }

    /// <summary>
    /// Creates a new order in PENDING status.
    /// </summary>
    public static Order Create(
        Guid tenantId,
        string symbol,
        string side,
        decimal quantity,
        decimal price)
    {
        return new Order
        {
            Id        = Guid.NewGuid(),
            TenantId  = tenantId,
            Symbol    = symbol,
            Side      = side,
            Quantity  = quantity,
            Price     = price,
            Status    = OrderStatus.Pending,
            CreatedAt = DateTime.UtcNow,
        };
    }

    /// <summary>
    /// Marks order as filled with actual execution price.
    /// </summary>
    public void MarkFilled(decimal filledPrice)
    {
        if (Status != OrderStatus.Pending)
            throw new InvalidOperationException(
                $"Cannot fill order in {Status} status.");

        Status    = OrderStatus.Filled;
        Price     = filledPrice;
        FilledAt  = DateTime.UtcNow;
    }

    /// <summary>
    /// Marks order as rejected with a reason.
    /// </summary>
    public void MarkRejected(string reason)
    {
        Status = OrderStatus.Rejected;
    }
}

public enum OrderStatus
{
    Pending,
    Validated,
    Filled,
    PartialFilled,
    Rejected,
    Cancelled,
}
```

### 4.4 CQRS — Commands and Queries

```csharp
// Application/Commands/PlaceOrderCommand.cs
using MediatR;

namespace TradingPlatform.OMS.Application.Commands;

/// <summary>
/// Command to place a new trade order after signal routing.
/// </summary>
public sealed record PlaceOrderCommand : IRequest<PlaceOrderResult>
{
    public required Guid    TenantId { get; init; }
    public required string  Symbol   { get; init; }
    public required string  Side     { get; init; }
    public required decimal Quantity { get; init; }
    public required string  SignalId { get; init; }
}

public sealed record PlaceOrderResult
{
    public required Guid        OrderId { get; init; }
    public required OrderStatus Status  { get; init; }
}

// Application/Handlers/PlaceOrderHandler.cs
namespace TradingPlatform.OMS.Application.Handlers;

/// <summary>
/// Handles PlaceOrderCommand — runs risk checks and persists order.
/// </summary>
public sealed class PlaceOrderHandler
    : IRequestHandler<PlaceOrderCommand, PlaceOrderResult>
{
    private readonly IOrderRepository _orderRepository;
    private readonly IRiskService     _riskService;
    private readonly IKafkaProducer   _kafkaProducer;
    private readonly ILogger<PlaceOrderHandler> _logger;

    public PlaceOrderHandler(
        IOrderRepository orderRepository,
        IRiskService     riskService,
        IKafkaProducer   kafkaProducer,
        ILogger<PlaceOrderHandler> logger)
    {
        _orderRepository = orderRepository;
        _riskService     = riskService;
        _kafkaProducer   = kafkaProducer;
        _logger          = logger;
    }

    public async Task<PlaceOrderResult> Handle(
        PlaceOrderCommand command,
        CancellationToken cancellationToken)
    {
        // Validate risk before creating order.
        RiskResult riskResult = await _riskService
            .ValidateAsync(command, cancellationToken);

        if (!riskResult.Passed)
        {
            _logger.LogWarning(
                "Risk check failed for tenant {TenantId}: {Reason}",
                command.TenantId,
                riskResult.Reason);

            return new PlaceOrderResult
            {
                OrderId = Guid.Empty,
                Status  = OrderStatus.Rejected,
            };
        }

        // Create and persist order.
        Order order = Order.Create(
            command.TenantId,
            command.Symbol,
            command.Side,
            command.Quantity,
            price: 0); // EMS will determine actual fill price.

        await _orderRepository.SaveAsync(order, cancellationToken);

        // Publish validated order for EMS to consume.
        await _kafkaProducer.PublishAsync(
            topic: "validated-orders",
            key:   command.TenantId.ToString(),
            value: order,
            cancellationToken);

        _logger.LogInformation(
            "Order {OrderId} created for tenant {TenantId}.",
            order.Id,
            command.TenantId);

        return new PlaceOrderResult
        {
            OrderId = order.Id,
            Status  = OrderStatus.Pending,
        };
    }
}
```

### 4.5 Repository Pattern

```csharp
// Domain/Interfaces/IOrderRepository.cs
namespace TradingPlatform.OMS.Domain.Interfaces;

/// <summary>
/// Persistence contract for Order aggregate.
/// </summary>
public interface IOrderRepository
{
    Task<Order?> GetByIdAsync(
        Guid id,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyList<Order>> GetByTenantAsync(
        Guid tenantId,
        CancellationToken cancellationToken = default);

    Task SaveAsync(
        Order order,
        CancellationToken cancellationToken = default);

    Task UpdateStatusAsync(
        Guid id,
        OrderStatus status,
        CancellationToken cancellationToken = default);
}

// Infrastructure/Repositories/OrderRepository.cs
namespace TradingPlatform.OMS.Infrastructure.Repositories;

/// <summary>
/// PostgreSQL implementation of IOrderRepository using EF Core.
/// </summary>
public sealed class OrderRepository : IOrderRepository
{
    private readonly TradingDbContext _context;

    public OrderRepository(TradingDbContext context)
    {
        _context = context;
    }

    public async Task<Order?> GetByIdAsync(
        Guid id,
        CancellationToken cancellationToken = default)
    {
        return await _context.Orders
            .FirstOrDefaultAsync(
                o => o.Id == id,
                cancellationToken);
    }

    public async Task SaveAsync(
        Order order,
        CancellationToken cancellationToken = default)
    {
        _context.Orders.Add(order);
        await _context.SaveChangesAsync(cancellationToken);
    }
}
```

### 4.6 Dependency Injection Registration

```csharp
// OMS.Api/Program.cs
using MediatR;
using TradingPlatform.OMS.Application.Handlers;
using TradingPlatform.OMS.Domain.Interfaces;
using TradingPlatform.OMS.Infrastructure.Repositories;

var builder = WebApplication.CreateBuilder(args);

// MediatR — registers all handlers automatically.
builder.Services.AddMediatR(cfg =>
    cfg.RegisterServicesFromAssembly(
        typeof(PlaceOrderHandler).Assembly));

// Repositories — scoped to request lifecycle.
builder.Services.AddScoped<IOrderRepository, OrderRepository>();
builder.Services.AddScoped<IRiskService, RiskService>();
builder.Services.AddScoped<IPositionRepository, PositionRepository>();

// Kafka — singleton, one connection reused app-wide.
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();

// Database.
builder.Services.AddDbContext<TradingDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("Postgres")));

// Observability.
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddEntityFrameworkCoreInstrumentation()
        .AddOtlpExporter());

builder.Services.AddControllers();

WebApplication app = builder.Build();

app.UseAuthentication();
app.UseAuthorization();
app.MapControllers();

app.Run();
```

### 4.7 Error Handling

```csharp
// WRONG — swallowing exception
public async Task<Order?> GetByIdAsync(Guid id)
{
    try
    {
        return await _context.Orders.FindAsync(id);
    }
    catch (Exception)
    {
        return null; // caller has no idea what went wrong.
    }
}

// CORRECT — specific, logged, meaningful exception
public async Task<Order?> GetByIdAsync(
    Guid id,
    CancellationToken cancellationToken = default)
{
    try
    {
        return await _context.Orders
            .FirstOrDefaultAsync(o => o.Id == id, cancellationToken);
    }
    catch (OperationCanceledException)
    {
        // Cancellation is expected — do not log as error.
        throw;
    }
    catch (DbUpdateException ex)
    {
        _logger.LogError(
            ex,
            "Database error fetching order {OrderId}.",
            id);

        throw new RepositoryException(
            $"Failed to retrieve order {id}.", ex);
    }
}
```

---

## 5. Component 1 — Exchange Connector (Python)

**Purpose:** Connect to Binance WebSocket, receive raw ticks, normalize, publish to Kafka.

**File:** `python/exchange_connector/src/connector.py`

```python
import asyncio
import json
import os
from datetime import datetime

import websockets
from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.models import Tick

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
SYMBOLS        = os.getenv("TRADE_SYMBOLS", "btcusdt,ethusdt").split(",")
KAFKA_TOPIC    = "market-data-raw"

logger = get_logger(__name__)


class ExchangeConnector:
    """Connects to Binance WebSocket and publishes ticks to Kafka."""

    def __init__(self) -> None:
        self._producer = KafkaClientFactory.create_producer("exchange-connector")

    async def run(self) -> None:
        """Start WebSocket connection and stream ticks indefinitely."""
        stream_url = self._build_stream_url()
        
        while True:
            try:
                await self._connect_and_stream(stream_url)
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(
                    "WebSocket connection closed, reconnecting.",
                    extra={"reason": str(e)}
                )
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(
                    "Unexpected error in exchange connector.",
                    extra={"error": str(e)}
                )
                await asyncio.sleep(10)

    async def _connect_and_stream(self, url: str) -> None:
        """Connect to WebSocket URL and process incoming messages."""
        async with websockets.connect(url) as websocket:
            logger.info("Connected to Binance WebSocket.")
            
            async for raw_message in websocket:
                tick = self._parse_and_normalize(raw_message)
                if tick is not None:
                    self._publish(tick)

    def _parse_and_normalize(self, raw: str) -> Tick | None:
        """Parse raw Binance message into normalized Tick model."""
        try:
            data = json.loads(raw)
            return Tick(
                symbol    = data["s"].upper(),
                price     = float(data["p"]),
                volume    = float(data["q"]),
                timestamp = datetime.utcfromtimestamp(data["T"] / 1000),
            )
        except (KeyError, ValueError) as e:
            logger.warning(
                "Failed to parse tick message.",
                extra={"error": str(e), "raw": raw[:100]}
            )
            return None

    def _publish(self, tick: Tick) -> None:
        """Publish normalized tick to Kafka market-data-raw topic."""
        self._producer.produce(
            topic    = KAFKA_TOPIC,
            key      = tick.symbol,
            value    = tick.model_dump_json(),
            callback = self._delivery_callback,
        )
        self._producer.poll(0)

    def _delivery_callback(self, err, msg) -> None:
        """Log Kafka delivery errors."""
        if err is not None:
            logger.error(
                "Kafka delivery failed.",
                extra={"error": str(err), "topic": KAFKA_TOPIC}
            )

    def _build_stream_url(self) -> str:
        """Build Binance multi-stream WebSocket URL for configured symbols."""
        streams = "/".join(f"{s}@trade" for s in SYMBOLS)
        return f"{BINANCE_WS_URL}/{streams}"


if __name__ == "__main__":
    connector = ExchangeConnector()
    asyncio.run(connector.run())
```

---

## 6. Component 2 — Candle Aggregator (Python)

**Purpose:** Consume raw ticks, aggregate into 5-minute OHLCV candles, publish to Kafka, store in TimescaleDB.

**File:** `python/candle_aggregator/src/aggregator.py`

```python
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta

from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.models import Candle, Tick

TIMEFRAME_MINUTES = int(os.getenv("CANDLE_TIMEFRAME_MINUTES", "5"))
INPUT_TOPIC       = "market-data-raw"
OUTPUT_TOPIC      = "market-data-candles"
CONSUMER_GROUP    = "candle-aggregator"

logger = get_logger(__name__)


class CandleAggregator:
    """Aggregates raw ticks into OHLCV candles and publishes them."""

    def __init__(self) -> None:
        self._consumer = KafkaClientFactory.create_consumer(
            group_id = CONSUMER_GROUP,
            topics   = [INPUT_TOPIC],
        )
        self._producer     = KafkaClientFactory.create_producer("candle-aggregator")
        self._tick_buffers: dict[str, list[Tick]] = defaultdict(list)
        self._window_start: dict[str, datetime]   = {}

    def run(self) -> None:
        """Consume ticks indefinitely and emit candles on window close."""
        logger.info("Candle aggregator started.")
        
        while True:
            message = self._consumer.poll(timeout=1.0)
            
            if message is None:
                continue
            
            if message.error():
                logger.error(
                    "Kafka consumer error.",
                    extra={"error": str(message.error())}
                )
                continue

            tick = self._deserialize(message.value())
            if tick is not None:
                self._process_tick(tick)

    def _process_tick(self, tick: Tick) -> None:
        """Add tick to buffer and emit candle when window closes."""
        symbol = tick.symbol
        
        # Initialize window start on first tick for this symbol.
        if symbol not in self._window_start:
            self._window_start[symbol] = self._floor_to_window(tick.timestamp)

        window_end = self._window_start[symbol] + timedelta(minutes=TIMEFRAME_MINUTES)

        if tick.timestamp >= window_end:
            # Window closed — build and publish candle.
            if self._tick_buffers[symbol]:
                candle = self._build_candle(symbol)
                self._publish_candle(candle)
            
            # Reset buffer for new window.
            self._tick_buffers[symbol] = []
            self._window_start[symbol] = self._floor_to_window(tick.timestamp)

        self._tick_buffers[symbol].append(tick)

    def _build_candle(self, symbol: str) -> Candle:
        """Build OHLCV candle from buffered ticks for a symbol."""
        ticks  = self._tick_buffers[symbol]
        prices = [t.price for t in ticks]
        
        return Candle(
            symbol    = symbol,
            open      = prices[0],
            high      = max(prices),
            low       = min(prices),
            close     = prices[-1],
            volume    = sum(t.volume for t in ticks),
            opened_at = self._window_start[symbol],
            closed_at = ticks[-1].timestamp,
        )

    def _publish_candle(self, candle: Candle) -> None:
        """Publish completed candle to Kafka output topic."""
        self._producer.produce(
            topic = OUTPUT_TOPIC,
            key   = candle.symbol,
            value = candle.model_dump_json(),
        )
        self._producer.flush()
        
        logger.info(
            "Candle published.",
            extra={
                "symbol": candle.symbol,
                "close":  candle.close,
                "volume": candle.volume,
            }
        )

    def _deserialize(self, raw: bytes) -> Tick | None:
        """Deserialize raw Kafka message bytes into Tick model."""
        try:
            return Tick.model_validate_json(raw)
        except Exception as e:
            logger.warning(
                "Failed to deserialize tick.",
                extra={"error": str(e)}
            )
            return None

    def _floor_to_window(self, dt: datetime) -> datetime:
        """Floor a datetime to the nearest timeframe window boundary."""
        minutes    = (dt.minute // TIMEFRAME_MINUTES) * TIMEFRAME_MINUTES
        return dt.replace(minute=minutes, second=0, microsecond=0)
```

---

## 7. Component 3 — ML Signal Pipeline (Python + Airflow)

**Purpose:** Airflow DAG triggers every 5 minutes, pulls candles, runs causal model, publishes signals.

**File:** `python/signal_pipeline/dags/signal_dag.py`

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from signal_pipeline.src.model import SignalGenerator
from signal_pipeline.src.features import FeatureBuilder
from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.models import TradeSide, TradeSignal

logger = get_logger(__name__)

SYMBOLS = ["BTCUSDT", "ETHUSDT"]

default_args = {
    "owner":            "trading-team",
    "retries":          1,
    "retry_delay":      timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=4),
}

dag = DAG(
    dag_id            = "signal_generation",
    default_args      = default_args,
    schedule_interval = "*/5 * * * *",
    start_date        = datetime(2024, 1, 1),
    catchup           = False,
    tags              = ["trading", "signals"],
)


def generate_signals_for_symbol(symbol: str) -> None:
    """Generate and publish trade signal for a single symbol."""
    generator = SignalGenerator()
    builder   = FeatureBuilder()
    producer  = KafkaClientFactory.create_producer("signal-pipeline")

    features = builder.build(symbol, lookback_candles=100)
    
    if features is None:
        logger.warning(
            "Insufficient candle data for signal generation.",
            extra={"symbol": symbol}
        )
        return

    signal = generator.predict(symbol, features)

    producer.produce(
        topic = "trade-signals",
        key   = signal.symbol,
        value = signal.model_dump_json(),
    )
    producer.flush()

    logger.info(
        "Signal published.",
        extra={
            "symbol":     signal.symbol,
            "side":       signal.side,
            "confidence": signal.confidence,
        }
    )


for symbol in SYMBOLS:
    PythonOperator(
        task_id         = f"generate_signal_{symbol.lower()}",
        python_callable = generate_signals_for_symbol,
        op_kwargs       = {"symbol": symbol},
        dag             = dag,
    )
```

---

## 8. Component 4 — Signal Router (Python)

**Purpose:** Route signals to correct tenants based on their strategy config, publish per-tenant order requests.

**File:** `python/signal_router/src/router.py`

```python
import json
import os
import uuid
from datetime import datetime

from shared.db_client import get_db_connection
from shared.kafka_client import KafkaClientFactory
from shared.logger import get_logger
from shared.models import TradeSignal

INPUT_TOPIC    = "trade-signals"
OUTPUT_TOPIC   = "order-requests"
CONSUMER_GROUP = "signal-router"

logger = get_logger(__name__)


class SignalRouter:
    """Routes trade signals to matching tenants as order requests."""

    def __init__(self) -> None:
        self._consumer = KafkaClientFactory.create_consumer(
            group_id = CONSUMER_GROUP,
            topics   = [INPUT_TOPIC],
        )
        self._producer = KafkaClientFactory.create_producer("signal-router")

    def run(self) -> None:
        """Consume signals and route to tenants indefinitely."""
        logger.info("Signal router started.")
        
        while True:
            message = self._consumer.poll(timeout=1.0)
            
            if message is None:
                continue
            
            if message.error():
                logger.error(
                    "Consumer error in signal router.",
                    extra={"error": str(message.error())}
                )
                continue

            signal = self._deserialize(message.value())
            if signal is not None:
                self._route_signal(signal)

    def _route_signal(self, signal: TradeSignal) -> None:
        """Find matching tenants and publish order request for each."""
        if self._is_stale(signal):
            logger.warning(
                "Stale signal discarded.",
                extra={"signal_id": signal.id, "symbol": signal.symbol}
            )
            return

        tenants = self._get_matching_tenants(signal.symbol)
        
        for tenant in tenants:
            self._publish_order_request(signal, tenant)

    def _publish_order_request(
        self,
        signal: TradeSignal,
        tenant: dict,
    ) -> None:
        """Build and publish a per-tenant order request to Kafka."""
        order_request = {
            "id":         str(uuid.uuid4()),
            "tenant_id":  tenant["tenant_id"],
            "symbol":     signal.symbol,
            "side":       signal.side.value,
            "quantity":   tenant["position_size"],
            "signal_id":  signal.id,
            "created_at": datetime.utcnow().isoformat(),
        }

        self._producer.produce(
            topic = OUTPUT_TOPIC,
            key   = tenant["tenant_id"],
            value = json.dumps(order_request),
        )
        self._producer.flush()

        logger.info(
            "Order request published.",
            extra={
                "tenant_id": tenant["tenant_id"],
                "symbol":    signal.symbol,
                "side":      signal.side.value,
            }
        )

    def _get_matching_tenants(self, symbol: str) -> list[dict]:
        """Query PostgreSQL for tenants with active strategy for symbol."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tenant_id, position_size
                    FROM tenant_strategies
                    WHERE symbol = %s
                      AND enabled = true
                    """,
                    (symbol,)
                )
                return [
                    {"tenant_id": row[0], "position_size": float(row[1])}
                    for row in cur.fetchall()
                ]

    def _is_stale(self, signal: TradeSignal) -> bool:
        """Check whether signal has passed its expiry time."""
        return datetime.utcnow() > signal.expires_at

    def _deserialize(self, raw: bytes) -> TradeSignal | None:
        """Deserialize Kafka message bytes into TradeSignal model."""
        try:
            return TradeSignal.model_validate_json(raw)
        except Exception as e:
            logger.warning(
                "Failed to deserialize signal.",
                extra={"error": str(e)}
            )
            return None
```

---

## 9. Component 5 — OMS Microservice (C# / .NET 8)

> Full DDD + CQRS + MediatR + Repository + DI structure shown in Section 4.
> Build using the patterns defined there. Key files to create:

```
TradingPlatform.OMS.Domain/
├── Entities/Order.cs              ← aggregate root (see 4.3)
├── Entities/Position.cs           ← position tracking
├── ValueObjects/Money.cs          ← immutable decimal wrapper
├── ValueObjects/Symbol.cs         ← validated trading symbol
└── Interfaces/
    ├── IOrderRepository.cs
    ├── IPositionRepository.cs
    └── IRiskService.cs

TradingPlatform.OMS.Application/
├── Commands/
│   ├── PlaceOrderCommand.cs        ← (see 4.4)
│   └── UpdateOrderStatusCommand.cs
├── Queries/
│   ├── GetOrderByIdQuery.cs
│   └── GetPositionsByTenantQuery.cs
└── Handlers/
    ├── PlaceOrderHandler.cs        ← (see 4.4)
    └── UpdateOrderStatusHandler.cs

TradingPlatform.OMS.Infrastructure/
├── Persistence/
│   ├── TradingDbContext.cs
│   └── Configurations/OrderConfiguration.cs
├── Repositories/
│   └── OrderRepository.cs          ← (see 4.5)
└── Kafka/
    ├── KafkaConsumerService.cs      ← hosted service reading order-requests
    └── KafkaProducer.cs

TradingPlatform.OMS.Api/
├── Controllers/OrdersController.cs
└── Program.cs                      ← (see 4.6)
```

### Kafka Consumer as Hosted Service

```csharp
// Infrastructure/Kafka/KafkaConsumerService.cs
namespace TradingPlatform.OMS.Infrastructure.Kafka;

/// <summary>
/// Background service that continuously consumes order-requests from Kafka.
/// Runs for the lifetime of the application.
/// </summary>
public sealed class KafkaConsumerService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly IConsumer<string, string> _consumer;

    public KafkaConsumerService(
        IServiceScopeFactory scopeFactory,
        ILogger<KafkaConsumerService> logger,
        IKafkaConsumerFactory consumerFactory)
    {
        _scopeFactory = scopeFactory;
        _logger       = logger;
        _consumer     = consumerFactory.Create("oms-consumer", ["order-requests"]);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("OMS Kafka consumer started.");

        await Task.Run(async () =>
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<string, string> result =
                        _consumer.Consume(stoppingToken);

                    await ProcessMessageAsync(result.Message.Value, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    // Expected on shutdown — exit cleanly.
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Error processing Kafka message in OMS.");
                    
                    // Brief pause before retrying to avoid tight loop on error.
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
            }
        }, stoppingToken);
    }

    private async Task ProcessMessageAsync(
        string messageValue,
        CancellationToken cancellationToken)
    {
        // Scoped so EF Core DbContext gets a fresh instance per message.
        using IServiceScope scope = _scopeFactory.CreateScope();
        IMediator mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

        PlaceOrderCommand command = JsonSerializer
            .Deserialize<PlaceOrderCommand>(messageValue)!;

        await mediator.Send(command, cancellationToken);
    }
}
```

---

## 10. Component 6 — EMS Microservice (C# / .NET 8)

```
TradingPlatform.EMS.Domain/
├── Entities/Execution.cs
└── Interfaces/
    ├── IExecutionRepository.cs
    └── IBinanceClient.cs

TradingPlatform.EMS.Application/
├── Commands/ExecuteOrderCommand.cs
└── Handlers/ExecuteOrderHandler.cs

TradingPlatform.EMS.Infrastructure/
├── Exchange/
│   ├── BinanceClient.cs         ← HMAC signing + REST calls
│   └── HmacSigner.cs
├── Kafka/
│   ├── KafkaConsumerService.cs  ← reads validated-orders
│   └── KafkaProducer.cs         ← publishes order-fills
└── Persistence/
    └── ExecutionRepository.cs
```

### HMAC Signing

```csharp
// Infrastructure/Exchange/HmacSigner.cs
namespace TradingPlatform.EMS.Infrastructure.Exchange;

/// <summary>
/// Signs Binance API requests using HMAC-SHA256.
/// All Binance trading endpoints require this signature.
/// </summary>
public static class HmacSigner
{
    /// <summary>
    /// Generates HMAC-SHA256 signature for a query string.
    /// </summary>
    public static string Sign(string queryString, string secretKey)
    {
        byte[] keyBytes  = Encoding.UTF8.GetBytes(secretKey);
        byte[] dataBytes = Encoding.UTF8.GetBytes(queryString);

        using HMACSHA256 hmac = new(keyBytes);
        byte[] hashBytes = hmac.ComputeHash(dataBytes);

        return BitConverter.ToString(hashBytes)
            .Replace("-", "")
            .ToLower();
    }
}
```

---

## 11. Component 7 — Kafka Setup

### Create Topics Script

```bash
#!/bin/bash
# infra/kafka/create-topics.sh

KAFKA_BROKER="kafka:9092"
TOPICS=(
    "market-data-raw:3:1"
    "market-data-candles:3:1"
    "trade-signals:3:1"
    "order-requests:3:1"
    "validated-orders:3:1"
    "order-fills:3:1"
)

for TOPIC_CONFIG in "${TOPICS[@]}"; do
    TOPIC=$(echo $TOPIC_CONFIG | cut -d: -f1)
    PARTITIONS=$(echo $TOPIC_CONFIG | cut -d: -f2)
    REPLICATION=$(echo $TOPIC_CONFIG | cut -d: -f3)
    
    kafka-topics.sh \
        --create \
        --if-not-exists \
        --bootstrap-server $KAFKA_BROKER \
        --topic $TOPIC \
        --partitions $PARTITIONS \
        --replication-factor $REPLICATION
    
    echo "Created topic: $TOPIC"
done
```

---

## 12. Component 8 — Observability

### OpenTelemetry in C#

```csharp
// Already shown in Program.cs (Section 4.6)
// Every request automatically traced.
// Traces exported to Elastic APM via OTLP.
```

### Structured Logging in C#

```csharp
// Add Serilog to Program.cs
builder.Host.UseSerilog((context, config) =>
    config
        .ReadFrom.Configuration(context.Configuration)
        .WriteTo.Console(new JsonFormatter())
        .WriteTo.Elasticsearch(new ElasticsearchSinkOptions(
            new Uri(context.Configuration["Elastic:Url"]!))
        {
            IndexFormat = "trading-logs-{0:yyyy.MM}",
        }));
```

---

## 13. Docker Compose — Full Local Stack

```yaml
# infra/docker-compose.yml
version: "3.9"

services:

  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.3.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

  postgres:
    image: timescale/timescaledb:latest-pg14
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB:       trading
      POSTGRES_USER:     trading
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./db/migrations:/docker-entrypoint-initdb.d

  redis:
    image: redis:7.0-alpine
    ports:
      - "6379:6379"

  airflow:
    image: apache/airflow:2.7.0
    depends_on: [postgres]
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://trading:${POSTGRES_PASSWORD}@postgres/trading
    volumes:
      - ../python/signal_pipeline/dags:/opt/airflow/dags
    ports:
      - "8080:8080"

  exchange-connector:
    build: ../python/exchange_connector
    depends_on: [kafka]
    environment:
      KAFKA_BROKERS:    kafka:9092
      TRADE_SYMBOLS:    btcusdt,ethusdt

  candle-aggregator:
    build: ../python/candle_aggregator
    depends_on: [kafka, postgres]
    environment:
      KAFKA_BROKERS:            kafka:9092
      CANDLE_TIMEFRAME_MINUTES: 5

  signal-router:
    build: ../python/signal_router
    depends_on: [kafka, postgres]
    environment:
      KAFKA_BROKERS: kafka:9092
      DATABASE_URL:  postgresql://trading:${POSTGRES_PASSWORD}@postgres/trading

  oms:
    build: ../dotnet/src/OMS/TradingPlatform.OMS.Api
    depends_on: [kafka, postgres]
    ports:
      - "5001:8080"
    environment:
      ConnectionStrings__Postgres: Host=postgres;Database=trading;Username=trading;Password=${POSTGRES_PASSWORD}
      Kafka__Brokers: kafka:9092

  ems:
    build: ../dotnet/src/EMS/TradingPlatform.EMS.Api
    depends_on: [kafka]
    ports:
      - "5002:8080"
    environment:
      Kafka__Brokers: kafka:9092

volumes:
  postgres_data:
```

---

## 14. Database Schemas

```sql
-- db/migrations/001_create_tenants.sql
CREATE TABLE tenants (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       VARCHAR(100) NOT NULL,
    api_key    TEXT NOT NULL,             -- encrypted at application level
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE tenant_strategies (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID NOT NULL REFERENCES tenants(id),
    symbol        VARCHAR(20) NOT NULL,
    position_size DECIMAL(18, 8) NOT NULL,
    enabled       BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(tenant_id, symbol)
);

-- db/migrations/002_create_orders.sql
CREATE TABLE orders (
    id          UUID PRIMARY KEY,
    tenant_id   UUID NOT NULL REFERENCES tenants(id),
    symbol      VARCHAR(20) NOT NULL,
    side        VARCHAR(4) NOT NULL,
    quantity    DECIMAL(18, 8) NOT NULL,
    price       DECIMAL(18, 8),
    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    signal_id   VARCHAR(100),
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    filled_at   TIMESTAMPTZ
);

CREATE INDEX idx_orders_tenant ON orders(tenant_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created ON orders(created_at DESC);

-- db/migrations/003_create_positions.sql
CREATE TABLE positions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id),
    symbol      VARCHAR(20) NOT NULL,
    quantity    DECIMAL(18, 8) NOT NULL DEFAULT 0,
    avg_price   DECIMAL(18, 8) NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(tenant_id, symbol)
);

-- TimescaleDB hypertable for candles (time-series).
CREATE TABLE candles (
    time        TIMESTAMPTZ NOT NULL,
    symbol      VARCHAR(20) NOT NULL,
    open        DECIMAL(18, 8) NOT NULL,
    high        DECIMAL(18, 8) NOT NULL,
    low         DECIMAL(18, 8) NOT NULL,
    close       DECIMAL(18, 8) NOT NULL,
    volume      DECIMAL(18, 8) NOT NULL,
    timeframe   VARCHAR(5) NOT NULL DEFAULT '5m'
);

SELECT create_hypertable('candles', 'time');
CREATE INDEX idx_candles_symbol_time ON candles(symbol, time DESC);
```

---

## 15. Environment Variables

```bash
# .env.example — copy to .env and fill in values

# Database
POSTGRES_PASSWORD=your_password_here

# Kafka
KAFKA_BROKERS=kafka:9092

# Exchange (use testnet keys for local development)
BINANCE_API_KEY=your_testnet_api_key
BINANCE_SECRET_KEY=your_testnet_secret_key
BINANCE_BASE_URL=https://testnet.binance.vision

# Symbols to trade
TRADE_SYMBOLS=btcusdt,ethusdt

# Signal settings
CANDLE_TIMEFRAME_MINUTES=5
SIGNAL_TTL_SECONDS=90
SIGNAL_MIN_CONFIDENCE=0.7

# Observability
ELASTIC_APM_URL=http://apm-server:8200
ELASTIC_URL=http://elasticsearch:9200
LOG_LEVEL=INFO

# Redis
REDIS_URL=redis://redis:6379
```

---

## Quick Start

```bash
# 1. Clone and setup
git clone https://github.com/your-org/trading-platform
cd trading-platform
cp .env.example .env
# fill in your .env values

# 2. Start infrastructure
cd infra
docker-compose up -d zookeeper kafka postgres redis

# 3. Create Kafka topics
docker exec -it infra_kafka_1 bash /kafka/create-topics.sh

# 4. Run DB migrations
docker exec -it infra_postgres_1 psql -U trading -d trading \
  -f /docker-entrypoint-initdb.d/001_create_tenants.sql

# 5. Start Python services
docker-compose up -d exchange-connector candle-aggregator signal-router

# 6. Start C# services
docker-compose up -d oms ems

# 7. Start Airflow
docker-compose up -d airflow

# 8. Verify everything running
docker-compose ps
```

---

> **Claude instruction:** When building this system, follow every code standard defined in Sections 3 and 4 strictly. Never deviate from the DRY principle, error handling patterns, or layer separation rules. Build one component at a time, write tests alongside each component, and verify Kafka message flow before moving to the next component.
