namespace TradingPlatform.OMS.Domain.Interfaces;

/// <summary>
/// Read-only Binance client used by the OMS reconciliation service to verify
/// whether a specific order was filled on the exchange.
/// Kept separate from the EMS execution client to maintain layer boundaries.
/// </summary>
public interface IBinanceAccountClient
{
    /// <summary>
    /// Queries the status of a Binance order by the client order ID we set at placement.
    /// Returns null when the order does not exist on Binance (was never placed).
    /// </summary>
    Task<BinanceOrderQueryResult?> GetOrderByClientIdAsync(
        string symbol,
        string clientOrderId,
        CancellationToken ct = default);
}

/// <summary>Binance order status returned by a query.</summary>
public sealed record BinanceOrderQueryResult
{
    /// <summary>Gets the Binance order status (FILLED, NEW, CANCELED, EXPIRED, etc.).</summary>
    public required string Status { get; init; }

    /// <summary>Gets the quantity that was actually executed.</summary>
    public required decimal ExecutedQty { get; init; }

    /// <summary>Gets the total quote asset spent (price × qty for market orders).</summary>
    public required decimal CummulativeQuoteQty { get; init; }

    /// <summary>Gets the volume-weighted average fill price.</summary>
    public decimal AvgFillPrice => ExecutedQty > 0
        ? CummulativeQuoteQty / ExecutedQty
        : 0m;

    /// <summary>Gets whether the order was fully filled on Binance.</summary>
    public bool IsFilled => string.Equals(Status, "FILLED", StringComparison.OrdinalIgnoreCase);
}
