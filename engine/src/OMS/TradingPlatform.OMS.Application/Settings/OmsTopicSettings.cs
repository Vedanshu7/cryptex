namespace TradingPlatform.OMS.Application.Settings;

/// <summary>
/// Kafka topic names for the OMS. Override per regional deployment via env vars:
///   OMS__Topics__InputTopic  = tokyo.order-requests
///   OMS__Topics__OutputTopic = tokyo.validated-orders
///   OMS__Topics__FillsTopic  = tokyo.order-fills
/// Defaults match the single-region topic names so existing deployments
/// require no configuration change.
/// </summary>
public sealed class OmsTopicSettings
{
    /// <summary>Topic this OMS instance consumes order requests from.</summary>
    public string InputTopic  { get; set; } = "order-requests";

    /// <summary>Topic this OMS instance publishes validated orders to (consumed by EMS).</summary>
    public string OutputTopic { get; set; } = "validated-orders";

    /// <summary>Topic this OMS instance consumes fill events from (published by EMS).</summary>
    public string FillsTopic  { get; set; } = "order-fills";
}
