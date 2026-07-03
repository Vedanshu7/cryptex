namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Ensures a set of Kafka topics exist before any consumer or producer uses them.
/// </summary>
public interface IKafkaTopicProvisioner
{
    /// <summary>
    /// Creates any topics in <paramref name="topics"/> that do not already exist.
    /// Idempotent — an existing topic is treated as success, not an error, so this
    /// is safe to call on every service startup.
    /// </summary>
    Task EnsureTopicsExistAsync(
        IReadOnlyList<TopicSpec> topics,
        CancellationToken cancellationToken = default);
}
