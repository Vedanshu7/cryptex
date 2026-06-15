using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Interfaces;
using TradingPlatform.OMS.Infrastructure.Kafka;
using TradingPlatform.OMS.Infrastructure.Persistence;

namespace TradingPlatform.OMS.UnitTests.Handlers;

public sealed class StuckOrderReconciliationServiceTests
{
    private static Order _MakePendingOrder(Guid? tenantId = null) =>
        Order.Create(tenantId ?? Guid.NewGuid(), "BTCUSDT", "BUY", 0.1m, null);

    private static Order _MakeValidatedOrder(Guid? tenantId = null)
    {
        Order order = _MakePendingOrder(tenantId);
        order.MarkValidated();
        return order;
    }

    private static (StuckOrderReconciliationService Service, Mock<IOrderRepository> Repo)
        _Build(IReadOnlyList<Order> stuckOrders)
    {
        Mock<IOrderRepository> repoMock = new();
        repoMock
            .Setup(r => r.GetStuckAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(stuckOrders);
        repoMock
            .Setup(r => r.UpdateAsync(It.IsAny<Order>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        ServiceCollection services = new();
        services.AddScoped<IOrderRepository>(_ => repoMock.Object);
        services.AddScoped<TenantContext>();
#pragma warning disable CA2000 // ServiceProvider lifetime is intentionally test-scoped
        ServiceProvider provider = services.BuildServiceProvider();
#pragma warning restore CA2000

        Mock<IServiceScope> scopeMock = new();
        scopeMock.Setup(s => s.ServiceProvider).Returns(provider);

        Mock<IServiceScopeFactory> factoryMock = new();
        factoryMock.Setup(f => f.CreateScope()).Returns(scopeMock.Object);

        StuckOrderReconciliationService svc = new(
            factoryMock.Object,
            NullLogger<StuckOrderReconciliationService>.Instance);

        return (svc, repoMock);
    }

    [Fact]
    public async Task ReconcileAsync_NoStuckOrders_DoesNotCallUpdate()
    {
        (StuckOrderReconciliationService svc, Mock<IOrderRepository> repo) = _Build([]);

        await svc.ReconcileForTestAsync(CancellationToken.None);

        repo.Verify(r => r.UpdateAsync(It.IsAny<Order>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ReconcileAsync_PendingOrder_CancelsAndUpdates()
    {
        Order order = _MakePendingOrder();
        (StuckOrderReconciliationService svc, Mock<IOrderRepository> repo) = _Build([order]);

        await svc.ReconcileForTestAsync(CancellationToken.None);

        order.Status.Should().Be(OrderStatus.Cancelled);
        repo.Verify(r => r.UpdateAsync(order, CancellationToken.None), Times.Once);
    }

    [Fact]
    public async Task ReconcileAsync_ValidatedOrder_CancelsAndUpdates()
    {
        Order order = _MakeValidatedOrder();
        (StuckOrderReconciliationService svc, Mock<IOrderRepository> repo) = _Build([order]);

        await svc.ReconcileForTestAsync(CancellationToken.None);

        order.Status.Should().Be(OrderStatus.Cancelled);
        repo.Verify(r => r.UpdateAsync(order, CancellationToken.None), Times.Once);
    }

    [Fact]
    public async Task ReconcileAsync_MultipleOrders_AllCancelled()
    {
        Order[] orders = [_MakePendingOrder(), _MakeValidatedOrder(), _MakePendingOrder()];
        (StuckOrderReconciliationService svc, Mock<IOrderRepository> repo) = _Build(orders);

        await svc.ReconcileForTestAsync(CancellationToken.None);

        orders.Should().AllSatisfy(o => o.Status.Should().Be(OrderStatus.Cancelled));
        repo.Verify(r => r.UpdateAsync(It.IsAny<Order>(), It.IsAny<CancellationToken>()), Times.Exactly(3));
    }

    [Fact]
    public async Task ReconcileAsync_AlreadyFilledOrder_SkippedWithoutThrowing()
    {
        Order order = _MakeValidatedOrder();
        order.MarkFilled(50_000m);  // terminal — MarkCancelled will throw internally
        (StuckOrderReconciliationService svc, Mock<IOrderRepository> repo) = _Build([order]);

        Func<Task> act = () => svc.ReconcileForTestAsync(CancellationToken.None);

        await act.Should().NotThrowAsync();
        repo.Verify(r => r.UpdateAsync(It.IsAny<Order>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ReconcileAsync_AlreadyRejectedOrder_SkippedWithoutThrowing()
    {
        Order order = _MakePendingOrder();
        order.MarkRejected();       // terminal — MarkCancelled will throw internally
        (StuckOrderReconciliationService svc, Mock<IOrderRepository> repo) = _Build([order]);

        Func<Task> act = () => svc.ReconcileForTestAsync(CancellationToken.None);

        await act.Should().NotThrowAsync();
        repo.Verify(r => r.UpdateAsync(It.IsAny<Order>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}
