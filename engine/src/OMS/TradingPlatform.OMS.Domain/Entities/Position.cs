namespace TradingPlatform.OMS.Domain.Entities;

/// <summary>
/// Tracks the current holding quantity and average cost basis for a
/// tenant/symbol pair. Updated atomically on each order fill.
/// </summary>
public sealed class Position
{
    /// <summary>Gets the position identifier.</summary>
    public Guid Id { get; private set; }

    /// <summary>Gets the owning tenant.</summary>
    public Guid TenantId { get; private set; }

    /// <summary>Gets the trading symbol (e.g. BTCUSDT).</summary>
    public string Symbol { get; private set; } = string.Empty;

    /// <summary>Gets the current quantity held.</summary>
    public decimal Quantity { get; private set; }

    /// <summary>Gets the volume-weighted average purchase price.</summary>
    public decimal AvgPrice { get; private set; }

    /// <summary>Gets the UTC timestamp of the last update.</summary>
    public DateTime UpdatedAt { get; private set; }

    private Position() { }

    /// <summary>Creates a new flat (zero) position for a tenant/symbol pair.</summary>
    public static Position CreateFlat(Guid tenantId, string symbol)
    {
        return new Position
        {
            Id        = Guid.NewGuid(),
            TenantId  = tenantId,
            Symbol    = symbol,
            Quantity  = 0m,
            AvgPrice  = 0m,
            UpdatedAt = DateTime.UtcNow,
        };
    }

    /// <summary>
    /// Applies a fill to update quantity and average price using VWAP logic.
    /// </summary>
    public void ApplyFill(string side, decimal quantity, decimal fillPrice)
    {
        ArgumentNullException.ThrowIfNull(side);

        if (side.Equals("BUY", StringComparison.OrdinalIgnoreCase))
        {
            decimal totalCost = (AvgPrice * Quantity) + (fillPrice * quantity);
            Quantity  += quantity;
            AvgPrice   = Quantity > 0 ? totalCost / Quantity : 0m;
        }
        else
        {
            Quantity  = Math.Max(0m, Quantity - quantity);
            // Average price unchanged on sell — cost basis tracks buy-side only.
        }

        UpdatedAt = DateTime.UtcNow;
    }
}
