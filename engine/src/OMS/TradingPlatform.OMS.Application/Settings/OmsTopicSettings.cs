namespace TradingPlatform.OMS.Application.Settings;

/// <summary>
/// Kafka topic names for the OMS.
///
/// Preferred configuration for a regional deployment: set only
///   OMS__Topics__Region = tokyo
/// and Program.cs derives InputTopic/OutputTopic/FillsTopic from it
/// (tokyo.order-requests, tokyo.validated-orders, tokyo.order-fills),
/// validating that "tokyo" exists in universe.json at startup — one region
/// name instead of three hand-spelled topic strings that can drift out of
/// sync with EMS's own topic names.
///
/// Explicit overrides are still honored for exceptional cases:
///   OMS__Topics__InputTopic  = tokyo.order-requests
///   OMS__Topics__OutputTopic = tokyo.validated-orders
///   OMS__Topics__FillsTopic  = tokyo.order-fills
/// Defaults (no Region, no override) match the single-region topic names so
/// the legacy `oms` deployment requires no configuration change.
/// </summary>
public sealed class OmsTopicSettings
{
    /// <summary>
    /// Region this OMS instance serves (e.g. "tokyo"). When set, Program.cs
    /// derives the three topic names below and validates the region exists
    /// in universe.json. Leave unset for the legacy single-region deployment.
    /// </summary>
    public string? Region { get; set; }

    /// <summary>Topic this OMS instance consumes order requests from.</summary>
    public string InputTopic  { get; set; } = "order-requests";

    /// <summary>Topic this OMS instance publishes validated orders to (consumed by EMS).</summary>
    public string OutputTopic { get; set; } = "validated-orders";

    /// <summary>Topic this OMS instance consumes fill events from (published by EMS).</summary>
    public string FillsTopic  { get; set; } = "order-fills";
}
