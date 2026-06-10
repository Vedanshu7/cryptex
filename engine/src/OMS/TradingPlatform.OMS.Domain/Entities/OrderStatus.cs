namespace TradingPlatform.OMS.Domain.Entities;

/// <summary>Lifecycle states for a trade order.</summary>
public enum OrderStatus
{
    Pending,
    Validated,
    Filled,
    PartialFilled,
    Rejected,
    Cancelled,
}
