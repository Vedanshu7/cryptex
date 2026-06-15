namespace TradingPlatform.EMS.Domain.Interfaces;

/// <summary>Abstraction over the Binance REST API for order execution.</summary>
public interface IBinanceClient
{
    /// <summary>
    /// Submits a market order to the Binance exchange.
    /// </summary>
    /// <param name="symbol">Trading pair (e.g. BTCUSDT).</param>
    /// <param name="side">BUY or SELL.</param>
    /// <param name="quantity">Order quantity.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <param name="clientOrderId">
    /// Optional OMS order ID passed as Binance newClientOrderId.
    /// Enables reconciliation to query a specific order by our own ID on failover.
    /// </param>
    /// <returns>The exchange-assigned order ID and actual fill price.</returns>
    Task<BinanceFillResult> PlaceMarketOrderAsync(
        string symbol,
        string side,
        decimal quantity,
        string? clientOrderId = null,
        CancellationToken ct = default);
}

/// <summary>Result returned by a successful Binance order submission.</summary>
public sealed record BinanceFillResult
{
    /// <summary>Gets the Binance-assigned numeric order ID.</summary>
    public required long OrderId { get; init; }

    /// <summary>Gets the actual weighted average fill price.</summary>
    public required decimal FillPrice { get; init; }
}
