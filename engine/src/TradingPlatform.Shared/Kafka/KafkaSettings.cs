namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Kafka connection settings bound from the Kafka configuration section.
/// </summary>
public sealed record KafkaSettings
{
    /// <summary>Gets the comma-separated list of broker addresses.</summary>
    public required string Brokers { get; init; }
}
