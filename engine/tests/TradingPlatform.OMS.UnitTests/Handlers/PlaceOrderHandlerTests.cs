using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using TradingPlatform.Common.Kafka;
using TradingPlatform.OMS.Application.Commands;
using TradingPlatform.OMS.Application.Handlers;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Interfaces;

namespace TradingPlatform.OMS.UnitTests.Handlers;

public sealed class PlaceOrderHandlerTests
{
    private readonly Mock<IOrderRepository> _orderRepository = new();
    private readonly Mock<IRiskService> _riskService = new();
    private readonly Mock<IKafkaProducer> _kafkaProducer = new();

    private PlaceOrderHandler CreateHandler() =>
        new(
            _orderRepository.Object,
            _riskService.Object,
            _kafkaProducer.Object,
            NullLogger<PlaceOrderHandler>.Instance);

    [Fact]
    public async Task Handle_RiskPass_SavesOrderAndPublishesToKafka()
    {
        _riskService
            .Setup(r => r.ValidateAsync(
                It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<decimal>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(RiskResult.Pass());

        PlaceOrderCommand command = new()
        {
            TenantId = Guid.NewGuid(),
            Symbol   = "BTCUSDT",
            Side     = "BUY",
            Quantity = 0.01m,
            SignalId = "sig-1",
        };

        PlaceOrderResult result = await CreateHandler().Handle(command, default);

        result.Status.Should().Be(OrderStatus.Validated);
        result.OrderId.Should().NotBe(Guid.Empty);

        _orderRepository.Verify(
            r => r.SaveAsync(It.IsAny<Order>(), It.IsAny<CancellationToken>()),
            Times.Once);

        _kafkaProducer.Verify(
            k => k.PublishAsync(
                "validated-orders",
                It.IsAny<string>(),
                It.IsAny<Order>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Handle_RiskFail_ReturnsRejectedWithNoSideEffects()
    {
        _riskService
            .Setup(r => r.ValidateAsync(
                It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<decimal>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(RiskResult.Fail("Quantity exceeds limit."));

        PlaceOrderCommand command = new()
        {
            TenantId = Guid.NewGuid(),
            Symbol   = "BTCUSDT",
            Side     = "BUY",
            Quantity = 999m,
            SignalId = "sig-2",
        };

        PlaceOrderResult result = await CreateHandler().Handle(command, default);

        result.Status.Should().Be(OrderStatus.Rejected);
        result.OrderId.Should().Be(Guid.Empty);
        result.RejectionReason.Should().Contain("Quantity");

        _orderRepository.Verify(
            r => r.SaveAsync(It.IsAny<Order>(), It.IsAny<CancellationToken>()),
            Times.Never);

        _kafkaProducer.Verify(
            k => k.PublishAsync<It.IsAnyType>(
                It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<It.IsAnyType>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Handle_NullCommand_ThrowsArgumentNullException()
    {
        Func<Task> act = () => CreateHandler()
            .Handle(null!, CancellationToken.None);

        await act.Should().ThrowAsync<ArgumentNullException>();
    }
}
