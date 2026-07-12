using FluentAssertions;
using Moq;
using TradingPlatform.Common.Kafka;

namespace TradingPlatform.OMS.UnitTests.Kafka;

public sealed class DeadLetterPublisherTests
{
    private readonly Mock<IKafkaProducer> _producer = new();

    private DeadLetterPublisher CreatePublisher() => new(_producer.Object);

    [Fact]
    public async Task PublishAsync_DerivesDlqTopicFromSourceTopic()
    {
        await CreatePublisher().PublishAsync(
            "tokyo.validated-orders", "key", "raw-value", new InvalidOperationException("boom"), 3);

        _producer.Verify(
            p => p.PublishAsync(
                "tokyo.validated-orders.dlq",
                "key",
                It.IsAny<DeadLetterEnvelope>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task PublishAsync_NullKey_UsesUnknownAsPublishKey()
    {
        await CreatePublisher().PublishAsync(
            "tokyo.validated-orders", null, "raw-value", new InvalidOperationException("boom"), 1);

        _producer.Verify(
            p => p.PublishAsync(
                "tokyo.validated-orders.dlq",
                "unknown",
                It.IsAny<DeadLetterEnvelope>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task PublishAsync_EnvelopeCarriesErrorAndAttemptDetails()
    {
        DeadLetterEnvelope? captured = null;
        _producer
            .Setup(p => p.PublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<DeadLetterEnvelope>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, DeadLetterEnvelope, CancellationToken>((_, _, envelope, _) => captured = envelope)
            .Returns(Task.CompletedTask);

        InvalidOperationException error = new("boom");
        await CreatePublisher().PublishAsync("tokyo.validated-orders", "key", "raw-value", error, 3);

        captured.Should().NotBeNull();
        captured!.SourceTopic.Should().Be("tokyo.validated-orders");
        captured.Key.Should().Be("key");
        captured.RawValue.Should().Be("raw-value");
        captured.ErrorType.Should().Be(nameof(InvalidOperationException));
        captured.ErrorMessage.Should().Be("boom");
        captured.Attempts.Should().Be(3);
    }
}
