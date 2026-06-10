using Confluent.Kafka;

namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Factory that creates pre-configured Kafka consumers.
/// Keeps consumer creation centralized so all services use identical settings.
/// </summary>
public interface IKafkaConsumerFactory
{
    /// <summary>
    /// Creates a new consumer subscribed to the given topics.
    /// </summary>
    /// <param name="groupId">Consumer group identifier for offset management.</param>
    /// <param name="topics">Topics to subscribe to immediately after creation.</param>
    IConsumer<string, string> Create(string groupId, IEnumerable<string> topics);
}
