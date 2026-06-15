namespace TradingPlatform.EMS.Application.Settings;

/// <summary>
/// Kafka topic names for the EMS. Override per regional deployment via env vars:
///   EMS__Topics__InputTopic  = tokyo.validated-orders
///   EMS__Topics__OutputTopic = tokyo.order-fills
/// Defaults match the single-region topic names so existing deployments
/// require no configuration change.
/// </summary>
public sealed class EmsTopicSettings
{
    /// <summary>Topic this EMS instance consumes validated orders from (published by OMS).</summary>
    public string InputTopic  { get; set; } = "validated-orders";

    /// <summary>Topic this EMS instance publishes fill events to (consumed by OMS).</summary>
    public string OutputTopic { get; set; } = "order-fills";
}
