namespace TradingPlatform.Common.Kafka;

/// <summary>Confluent.Kafka-backed implementation of <see cref="IDeadLetterPublisher"/>.</summary>
public sealed class DeadLetterPublisher : IDeadLetterPublisher
{
    private readonly IKafkaProducer _producer;

    /// <summary>Initializes the publisher, reusing the shared <see cref="IKafkaProducer"/>.</summary>
    public DeadLetterPublisher(IKafkaProducer producer)
    {
        ArgumentNullException.ThrowIfNull(producer);
        _producer = producer;
    }

    /// <inheritdoc/>
    public Task PublishAsync(
        string sourceTopic,
        string? key,
        string rawValue,
        Exception exception,
        int attempts,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sourceTopic);
        ArgumentNullException.ThrowIfNull(rawValue);
        ArgumentNullException.ThrowIfNull(exception);

        DeadLetterEnvelope envelope = new(
            SourceTopic: sourceTopic,
            Key: key,
            RawValue: rawValue,
            ErrorType: exception.GetType().Name,
            ErrorMessage: exception.Message,
            Attempts: attempts,
            FailedAtUtc: DateTimeOffset.UtcNow);

        return _producer.PublishAsync(
            $"{sourceTopic}.dlq",
            key ?? "unknown",
            envelope,
            cancellationToken);
    }
}
