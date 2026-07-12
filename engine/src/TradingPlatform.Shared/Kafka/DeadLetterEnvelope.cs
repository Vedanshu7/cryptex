namespace TradingPlatform.Common.Kafka;

/// <summary>Payload published to <c>{sourceTopic}.dlq</c> for a quarantined message.</summary>
public sealed record DeadLetterEnvelope(
    string SourceTopic,
    string? Key,
    string RawValue,
    string ErrorType,
    string ErrorMessage,
    int Attempts,
    DateTimeOffset FailedAtUtc);
