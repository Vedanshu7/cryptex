using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using TradingPlatform.Common.Exceptions;
using TradingPlatform.Common.Kafka;

namespace TradingPlatform.OMS.UnitTests.Kafka;

public sealed class RetryingDlqDispatcherTests
{
    private readonly Mock<IDeadLetterPublisher> _dlq = new();

    private RetryingDlqDispatcher CreateDispatcher() =>
        new(_dlq.Object, NullLogger<RetryingDlqDispatcher>.Instance, serviceName: "oms");

    [Fact]
    public async Task DispatchAsync_SucceedsFirstTry_NeverPublishesToDlq()
    {
        Task ProcessOnce(CancellationToken ct) => Task.CompletedTask;

        await CreateDispatcher().DispatchAsync("tokyo.validated-orders", "key", "value", ProcessOnce);

        _dlq.Verify(
            d => d.PublishAsync(
                It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<string>(),
                It.IsAny<Exception>(), It.IsAny<int>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task DispatchAsync_RetryableExceptionThenSuccess_RetriesAndPublishesNothing()
    {
        int attempts = 0;
        Task ProcessOnce(CancellationToken ct)
        {
            attempts++;
            if (attempts < 2)
            {
                throw new RetryableProcessingException("transient DB blip");
            }

            return Task.CompletedTask;
        }

        await CreateDispatcher().DispatchAsync("tokyo.validated-orders", "key", "value", ProcessOnce);

        attempts.Should().Be(2);
        _dlq.Verify(
            d => d.PublishAsync(
                It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<string>(),
                It.IsAny<Exception>(), It.IsAny<int>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task DispatchAsync_RetryableExceptionExhausted_PublishesToDlqAfterMaxAttempts()
    {
        int attempts = 0;
        Task ProcessOnce(CancellationToken ct)
        {
            attempts++;
            throw new RetryableProcessingException("still down");
        }

        await CreateDispatcher().DispatchAsync("tokyo.validated-orders", "key", "value", ProcessOnce);

        attempts.Should().Be(3);
        _dlq.Verify(
            d => d.PublishAsync(
                "tokyo.validated-orders", "key", "value",
                It.IsAny<RetryableProcessingException>(), 3, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task DispatchAsync_NonRetryableException_PublishesToDlqImmediatelyNoRetry()
    {
        int attempts = 0;
        Task ProcessOnce(CancellationToken ct)
        {
            attempts++;
            throw new NonRetryableProcessingException("bad payload");
        }

        await CreateDispatcher().DispatchAsync("tokyo.validated-orders", "key", "value", ProcessOnce);

        attempts.Should().Be(1);
        _dlq.Verify(
            d => d.PublishAsync(
                "tokyo.validated-orders", "key", "value",
                It.IsAny<NonRetryableProcessingException>(), 1, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task DispatchAsync_JsonException_PublishesToDlqImmediatelyNoRetry()
    {
        int attempts = 0;
        Task ProcessOnce(CancellationToken ct)
        {
            attempts++;
            throw new JsonException("malformed payload");
        }

        await CreateDispatcher().DispatchAsync("tokyo.validated-orders", "key", "value", ProcessOnce);

        attempts.Should().Be(1);
        _dlq.Verify(
            d => d.PublishAsync(
                "tokyo.validated-orders", "key", "value",
                It.IsAny<JsonException>(), 1, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task DispatchAsync_UnrelatedExceptionType_PropagatesUncaught()
    {
        Task ProcessOnce(CancellationToken ct) => throw new InvalidOperationException("genuine bug");

        Func<Task> act = () => CreateDispatcher()
            .DispatchAsync("tokyo.validated-orders", "key", "value", ProcessOnce);

        await act.Should().ThrowAsync<InvalidOperationException>();

        _dlq.Verify(
            d => d.PublishAsync(
                It.IsAny<string>(), It.IsAny<string?>(), It.IsAny<string>(),
                It.IsAny<Exception>(), It.IsAny<int>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }
}
