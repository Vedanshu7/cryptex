namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Message published to the order-fills topic by the EMS after a successful exchange execution.
/// Consumed by the OMS to update order status and positions.
/// </summary>
public sealed record OrderFillEvent
{
    /// <summary>Gets the OMS order ID that was executed.</summary>
    public required Guid OrderId { get; init; }

    /// <summary>Gets the owning tenant.</summary>
    public required Guid TenantId { get; init; }

    /// <summary>Gets the actual weighted average fill price from the exchange.</summary>
    public required decimal FillPrice { get; init; }

    /// <summary>Gets the Binance-assigned order ID for reconciliation.</summary>
    public long? ExchangeOrderId { get; init; }
}
