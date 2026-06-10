using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Creates pre-configured Confluent.Kafka consumers.
/// </summary>
public sealed class KafkaConsumerFactory : IKafkaConsumerFactory
{
    private readonly KafkaSettings _settings;

    /// <summary>Initializes the factory with Kafka settings.</summary>
    public KafkaConsumerFactory(IOptions<KafkaSettings> settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        _settings = settings.Value;
    }

    /// <inheritdoc/>
    public IConsumer<string, string> Create(
        string groupId,
        IEnumerable<string> topics)
    {
        ConsumerConfig config = new()
        {
            BootstrapServers = _settings.Brokers,
            GroupId          = groupId,
            AutoOffsetReset  = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
            SessionTimeoutMs = 30_000,
        };

        IConsumer<string, string> consumer =
            new ConsumerBuilder<string, string>(config).Build();

        consumer.Subscribe(topics);
        return consumer;
    }
}
