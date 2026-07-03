namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Declarative spec for a Kafka topic a service depends on existing at startup.
/// Partition/replication defaults match infra/kafka/create-topics.sh.
/// </summary>
public sealed record TopicSpec
{
    /// <summary>Topic name.</summary>
    public required string Name { get; init; }

    /// <summary>Partition count.</summary>
    public int Partitions { get; init; } = 3;

    /// <summary>Replication factor. 1 for local dev single-broker clusters.</summary>
    public short ReplicationFactor { get; init; } = 1;
}
