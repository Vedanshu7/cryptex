using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using TradingPlatform.Common.Kafka;
using TradingPlatform.EMS.Application.Commands;
using TradingPlatform.EMS.Application.Handlers;
using TradingPlatform.EMS.Domain.Entities;
using TradingPlatform.EMS.Domain.Interfaces;

namespace TradingPlatform.EMS.UnitTests.Handlers;

public sealed class ExecuteOrderHandlerTests
{
    private readonly Mock<IBinanceClient> _binanceClient = new();
    private readonly Mock<IExecutionRepository> _executionRepository = new();
    private readonly Mock<IKafkaProducer> _kafkaProducer = new();

    private ExecuteOrderHandler CreateHandler() =>
        new(
            _binanceClient.Object,
            _executionRepository.Object,
            _kafkaProducer.Object,
            NullLogger<ExecuteOrderHandler>.Instance);

    private static ExecuteOrderCommand ValidCommand() => new()
    {
        OrderId  = Guid.NewGuid(),
        TenantId = Guid.NewGuid(),
        Symbol   = "BTCUSDT",
        Side     = "BUY",
        Quantity = 0.01m,
    };

    [Fact]
    public async Task Handle_SuccessfulFill_PublishesFillAndReturnsSuccess()
    {
        _binanceClient
            .Setup(b => b.PlaceMarketOrderAsync(
                It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<decimal>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new BinanceFillResult { OrderId = 12345, FillPrice = 50_000m });

        ExecuteOrderResult result = await CreateHandler().Handle(ValidCommand(), default);

        result.Success.Should().BeTrue();
        result.FillPrice.Should().Be(50_000m);
        result.ExchangeOrderId.Should().Be(12345);

        _executionRepository.Verify(
            r => r.SaveAsync(
                It.Is<Execution>(e => e.Status == ExecutionStatus.Filled),
                It.IsAny<CancellationToken>()),
            Times.Once);

        _kafkaProducer.Verify(
            k => k.PublishAsync(
                "order-fills", It.IsAny<string>(),
                It.IsAny<object>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Handle_BinanceRejectsOrder_ReturnsFailureWithErrorMessage()
    {
        _binanceClient
            .Setup(b => b.PlaceMarketOrderAsync(
                It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<decimal>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Insufficient balance."));

        ExecuteOrderResult result = await CreateHandler().Handle(ValidCommand(), default);

        result.Success.Should().BeFalse();
        result.ErrorMessage.Should().Contain("Insufficient");

        _executionRepository.Verify(
            r => r.SaveAsync(
                It.Is<Execution>(e => e.Status == ExecutionStatus.Error),
                It.IsAny<CancellationToken>()),
            Times.Once);

        _kafkaProducer.Verify(
            k => k.PublishAsync<It.IsAnyType>(
                It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<It.IsAnyType>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Handle_NullCommand_ThrowsArgumentNullException()
    {
        Func<Task> act = () => CreateHandler().Handle(null!, CancellationToken.None);
        await act.Should().ThrowAsync<ArgumentNullException>();
    }
}
