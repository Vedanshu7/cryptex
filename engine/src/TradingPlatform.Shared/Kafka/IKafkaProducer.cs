namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Abstraction for publishing messages to a Kafka topic.
/// Singleton lifetime — one connection reused across all operations.
/// </summary>
public interface IKafkaProducer
{
    /// <summary>
    /// Serializes <paramref name="value"/> as JSON and publishes it to <paramref name="topic"/>.
    /// </summary>
    /// <param name="topic">Target Kafka topic name.</param>
    /// <param name="key">Partition key — use tenant_id for tenant-affinity routing.</param>
    /// <param name="value">Payload to serialize. Must be JSON-serializable.</param>
    /// <param name="cancellationToken">Propagated from the calling hosted service.</param>
    Task PublishAsync<T>(
        string topic,
        string key,
        T value,
        CancellationToken cancellationToken = default)
        where T : notnull;
}
