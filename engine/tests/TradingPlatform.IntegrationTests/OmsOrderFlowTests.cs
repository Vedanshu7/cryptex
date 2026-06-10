using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Testcontainers.PostgreSql;
using TradingPlatform.Common.Kafka;
using TradingPlatform.OMS.Application.Commands;
using TradingPlatform.OMS.Application.Handlers;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Interfaces;
using TradingPlatform.OMS.Infrastructure.Persistence;
using TradingPlatform.OMS.Infrastructure.Repositories;
using TradingPlatform.OMS.Infrastructure.Services;

namespace TradingPlatform.IntegrationTests;

/// <summary>
/// End-to-end order placement tests against a real PostgreSQL container.
/// Validates the full flow: command → handler → repository → database.
/// </summary>
// IAsyncLifetime.DisposeAsync handles cleanup — CA1001 is a false positive here.
#pragma warning disable CA1001
public sealed class OmsOrderFlowTests : IAsyncLifetime
#pragma warning restore CA1001
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .WithDatabase("trading_test")
        .WithUsername("trading")
        .WithPassword("test_password")
        .Build();

    private TradingDbContext _dbContext = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

        DbContextOptions<TradingDbContext> options = new DbContextOptionsBuilder<TradingDbContext>()
            .UseNpgsql(_postgres.GetConnectionString())
            .Options;

        // Pass a null-tenant context so global query filters are bypassed (tenant_id == null → no filter).
        _dbContext = new TradingDbContext(options, new TenantContext());

        // Create schema directly — migrations are tested separately.
        await _dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS orders (
                id          UUID          PRIMARY KEY,
                tenant_id   UUID          NOT NULL,
                symbol      VARCHAR(20)   NOT NULL,
                side        VARCHAR(4)    NOT NULL,
                quantity    DECIMAL(18,8) NOT NULL,
                price       DECIMAL(18,8),
                status      VARCHAR(20)   NOT NULL DEFAULT 'Pending',
                signal_id   VARCHAR(100),
                created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
                filled_at   TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS positions (
                id         UUID          PRIMARY KEY,
                tenant_id  UUID          NOT NULL,
                symbol     VARCHAR(20)   NOT NULL,
                quantity   DECIMAL(18,8) NOT NULL DEFAULT 0,
                avg_price  DECIMAL(18,8) NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
                UNIQUE (tenant_id, symbol)
            );
            """);
    }

    public async Task DisposeAsync()
    {
        await _dbContext.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task PlaceOrder_RiskPass_OrderPersistedWithValidatedStatus()
    {
        IOrderRepository orderRepo = new OrderRepository(
            _dbContext,
            NullLogger<OrderRepository>.Instance);

        IPositionRepository positionRepo = new PositionRepository(_dbContext);
        IRiskService riskService        = new BasicRiskService();
        Mock<IKafkaProducer> kafka      = new();

        PlaceOrderHandler handler = new(
            orderRepo, riskService, kafka.Object,
            NullLogger<PlaceOrderHandler>.Instance);

        Guid tenantId = Guid.NewGuid();
        PlaceOrderCommand command = new()
        {
            TenantId = tenantId,
            Symbol   = "BTCUSDT",
            Side     = "BUY",
            Quantity = 0.01m,
            SignalId = "sig-integration-1",
        };

        PlaceOrderResult result = await handler.Handle(command, default);

        result.Status.Should().Be(OrderStatus.Validated);
        result.OrderId.Should().NotBe(Guid.Empty);

        // Verify the order is actually in the database.
        Order? persisted = await _dbContext.Orders
            .FirstOrDefaultAsync(o => o.Id == result.OrderId);

        persisted.Should().NotBeNull();
        persisted!.TenantId.Should().Be(tenantId);
        persisted.Symbol.Should().Be("BTCUSDT");
        persisted.Status.Should().Be(OrderStatus.Validated);

        kafka.Verify(
            k => k.PublishAsync(
                "validated-orders", tenantId.ToString(),
                It.IsAny<Order>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task PlaceOrder_RiskFail_OrderNotInDatabase()
    {
        IOrderRepository orderRepo = new OrderRepository(
            _dbContext,
            NullLogger<OrderRepository>.Instance);

        // Use a mock risk service that always rejects.
        Mock<IRiskService> riskService = new();
        riskService
            .Setup(r => r.ValidateAsync(
                It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<decimal>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(RiskResult.Fail("Test rejection."));

        Mock<IKafkaProducer> kafka = new();

        PlaceOrderHandler handler = new(
            orderRepo, riskService.Object, kafka.Object,
            NullLogger<PlaceOrderHandler>.Instance);

        PlaceOrderCommand command = new()
        {
            TenantId = Guid.NewGuid(),
            Symbol   = "ETHUSDT",
            Side     = "SELL",
            Quantity = 1.0m,
            SignalId = "sig-reject",
        };

        PlaceOrderResult result = await handler.Handle(command, default);

        result.Status.Should().Be(OrderStatus.Rejected);

        int ordersInDb = await _dbContext.Orders.CountAsync();
        ordersInDb.Should().Be(0);
    }
}
