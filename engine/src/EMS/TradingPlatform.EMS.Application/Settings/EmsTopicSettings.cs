namespace TradingPlatform.EMS.Application.Settings;

/// <summary>
/// Kafka topic names for the EMS.
///
/// Preferred configuration for a regional deployment: set only
///   EMS__Topics__Region = tokyo
/// and Program.cs derives InputTopic/OutputTopic from it
/// (tokyo.validated-orders, tokyo.order-fills), validating that "tokyo"
/// exists in universe.json at startup — one region name instead of two
/// hand-spelled topic strings that can drift out of sync with OMS's own
/// topic names.
///
/// Explicit overrides are still honored for exceptional cases:
///   EMS__Topics__InputTopic  = tokyo.validated-orders
///   EMS__Topics__OutputTopic = tokyo.order-fills
/// Defaults (no Region, no override) match the single-region topic names so
/// the legacy `ems` deployment requires no configuration change.
/// </summary>
public sealed class EmsTopicSettings
{
    /// <summary>
    /// Region this EMS instance serves (e.g. "tokyo"). When set, Program.cs
    /// derives the two topic names below and validates the region exists
    /// in universe.json. Leave unset for the legacy single-region deployment.
    /// </summary>
    public string? Region { get; set; }

    /// <summary>Topic this EMS instance consumes validated orders from (published by OMS).</summary>
    public string InputTopic  { get; set; } = "validated-orders";

    /// <summary>Topic this EMS instance publishes fill events to (consumed by OMS).</summary>
    public string OutputTopic { get; set; } = "order-fills";
}
