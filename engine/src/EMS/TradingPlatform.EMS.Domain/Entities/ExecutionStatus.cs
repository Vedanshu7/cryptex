namespace TradingPlatform.EMS.Domain.Entities;

/// <summary>Outcome states for a Binance order execution attempt.</summary>
public enum ExecutionStatus
{
    Submitted,
    Filled,
    Rejected,
    Error,
}
