using Prometheus;

namespace TradingPlatform.Common.Kafka;

/// <summary>
/// DLQ/retry counters shared by every <see cref="RetryingDlqDispatcher"/>
/// instance (EMS and OMS alike). Uses the exact same metric names as the
/// Python pipeline's <c>shared/metrics.py</c> (pipeline_dlq_messages_total,
/// pipeline_retry_attempts_total) so one Prometheus alert rule
/// (dlq-and-retries.yml) covers both stacks without duplicating it per
/// language. Exposed automatically through the existing /metrics endpoint —
/// prometheus-net's default registry is process-wide, no extra wiring needed
/// beyond referencing the package.
/// </summary>
public static class DlqMetrics
{
    private static readonly Counter DlqMessagesTotal = Metrics.CreateCounter(
        "pipeline_dlq_messages_total",
        "Total messages routed to a dead-letter topic.",
        new CounterConfiguration { LabelNames = ["service", "source_topic", "error_type"] });

    private static readonly Counter RetryAttemptsTotal = Metrics.CreateCounter(
        "pipeline_retry_attempts_total",
        "Total retry attempts made after a transient failure.",
        new CounterConfiguration { LabelNames = ["service", "operation"] });

    /// <summary>Records one message quarantined to a dead-letter topic.</summary>
    public static void RecordDlqMessage(string service, string sourceTopic, string errorType) =>
        DlqMessagesTotal.WithLabels(service, sourceTopic, errorType).Inc();

    /// <summary>Records one retry attempt made after a transient (retryable) failure.</summary>
    public static void RecordRetryAttempt(string service, string operation) =>
        RetryAttemptsTotal.WithLabels(service, operation).Inc();
}
