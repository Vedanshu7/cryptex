namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Runs a single Kafka message's processing logic with bounded retry, routing
/// to a dead-letter topic on exhaustion or a non-retryable failure.
/// </summary>
public interface IRetryingDlqDispatcher
{
    /// <summary>
    /// Invokes <paramref name="processOnce"/>, retrying on
    /// <c>RetryableProcessingException</c> with backoff. A
    /// <c>NonRetryableProcessingException</c> or a
    /// <see cref="System.Text.Json.JsonException"/> is quarantined immediately
    /// with no retry. Any other exception type is not caught here — it
    /// propagates to the caller, which is the intended fail-fast behavior for
    /// a genuine bug rather than a poison message.
    /// </summary>
    /// <param name="sourceTopic">Topic the message was consumed from — also the DLQ topic's prefix.</param>
    /// <param name="key">Original message key, if any.</param>
    /// <param name="rawValue">Original message value, for DLQ publication verbatim.</param>
    /// <param name="processOnce">The processing logic to attempt (deserialize + dispatch).</param>
    /// <param name="cancellationToken">Propagated from the calling consumer loop.</param>
    Task DispatchAsync(
        string sourceTopic,
        string? key,
        string rawValue,
        Func<CancellationToken, Task> processOnce,
        CancellationToken cancellationToken = default);
}
