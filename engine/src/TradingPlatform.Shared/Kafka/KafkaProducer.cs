using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Confluent.Kafka implementation of <see cref="IKafkaProducer"/>.
/// Serializes values to JSON before publishing.
/// </summary>
public sealed partial class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducer> _logger;

    /// <summary>Initializes the producer with settings from <see cref="KafkaSettings"/>.</summary>
    public KafkaProducer(
        IOptions<KafkaSettings> settings,
        ILogger<KafkaProducer> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(logger);

        _logger = logger;

        ProducerConfig config = new()
        {
            BootstrapServers  = settings.Value.Brokers,
            Acks              = Acks.All,    // all ISR replicas must acknowledge.
            MessageTimeoutMs  = 30_000,
            EnableDeliveryReports = true,
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    /// <inheritdoc/>
    public async Task PublishAsync<T>(
        string topic,
        string key,
        T value,
        CancellationToken cancellationToken = default)
        where T : notnull
    {
        string json = JsonSerializer.Serialize(value);

        Message<string, string> message = new()
        {
            Key   = key,
            Value = json,
        };

        try
        {
            DeliveryResult<string, string> result =
                await _producer
                    .ProduceAsync(topic, message, cancellationToken)
                    .ConfigureAwait(false);

            LogDelivered(_logger, result.Topic, result.Partition.Value);
        }
        catch (ProduceException<string, string> ex)
        {
            LogDeliveryFailed(_logger, topic, key, ex);
            throw;
        }
    }

    /// <inheritdoc/>
    public void Dispose() => _producer.Dispose();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Message delivered to {Topic} partition {Partition}.")]
    private static partial void LogDelivered(ILogger logger, string topic, int partition);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to deliver message to topic {Topic} with key {Key}.")]
    private static partial void LogDeliveryFailed(ILogger logger, string topic, string key, Exception ex);
}
