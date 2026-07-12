namespace TradingPlatform.Common.Kafka;

/// <summary>Publishes a quarantined message to its source topic's <c>.dlq</c> companion.</summary>
public interface IDeadLetterPublisher
{
    /// <summary>
    /// Publishes a <see cref="DeadLetterEnvelope"/> describing the failure to
    /// <c>{sourceTopic}.dlq</c>.
    /// </summary>
    /// <param name="sourceTopic">Topic the original message was consumed from.</param>
    /// <param name="key">Original message key, if any.</param>
    /// <param name="rawValue">Original message value, verbatim.</param>
    /// <param name="exception">The exception that caused this message to be quarantined.</param>
    /// <param name="attempts">Number of processing attempts made before quarantining.</param>
    /// <param name="cancellationToken">Propagated from the calling consumer loop.</param>
    Task PublishAsync(
        string sourceTopic,
        string? key,
        string rawValue,
        Exception exception,
        int attempts,
        CancellationToken cancellationToken = default);
}
