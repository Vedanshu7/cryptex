namespace TradingPlatform.OMS.Domain.Entities;

/// <summary>
/// Aggregate root representing a single trade order.
/// Encapsulates all state transitions for the order lifecycle.
/// Use <see cref="Create"/> to instantiate — never use the constructor directly.
/// </summary>
public sealed class Order
{
    /// <summary>Gets the unique order identifier.</summary>
    public Guid Id { get; private set; }

    /// <summary>Gets the tenant that placed this order.</summary>
    public Guid TenantId { get; private set; }

    /// <summary>Gets the trading symbol (e.g. BTCUSDT).</summary>
    public string Symbol { get; private set; } = string.Empty;

    /// <summary>Gets the order direction (BUY or SELL).</summary>
    public string Side { get; private set; } = string.Empty;

    /// <summary>Gets the requested quantity.</summary>
    public decimal Quantity { get; private set; }

    /// <summary>Gets the actual fill price (0 until filled).</summary>
    public decimal Price { get; private set; }

    /// <summary>Gets the current lifecycle status.</summary>
    public OrderStatus Status { get; private set; }

    /// <summary>Gets the ID of the signal that triggered this order.</summary>
    public string? SignalId { get; private set; }

    /// <summary>Gets the UTC creation timestamp.</summary>
    public DateTime CreatedAt { get; private set; }

    /// <summary>Gets the UTC fill timestamp, or null if not yet filled.</summary>
    public DateTime? FilledAt { get; private set; }

    // Private constructor — use Create() factory method.
    private Order() { }

    /// <summary>
    /// Creates a new order in <see cref="OrderStatus.Pending"/> status.
    /// </summary>
    public static Order Create(
        Guid tenantId,
        string symbol,
        string side,
        decimal quantity,
        string? signalId)
    {
        return new Order
        {
            Id        = Guid.NewGuid(),
            TenantId  = tenantId,
            Symbol    = symbol,
            Side      = side,
            Quantity  = quantity,
            Price     = 0m,
            Status    = OrderStatus.Pending,
            SignalId  = signalId,
            CreatedAt = DateTime.UtcNow,
        };
    }

    /// <summary>
    /// Marks this order as validated and ready for execution.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the order is not in <see cref="OrderStatus.Pending"/> status.
    /// </exception>
    public void MarkValidated()
    {
        if (Status != OrderStatus.Pending)
        {
            throw new InvalidOperationException(
                $"Cannot validate order {Id} in {Status} status.");
        }

        Status = OrderStatus.Validated;
    }

    /// <summary>
    /// Marks this order as filled with the actual execution price.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the order is not in <see cref="OrderStatus.Validated"/> status.
    /// </exception>
    public void MarkFilled(decimal filledPrice)
    {
        if (Status != OrderStatus.Validated)
        {
            throw new InvalidOperationException(
                $"Cannot fill order {Id} in {Status} status.");
        }

        Status   = OrderStatus.Filled;
        Price    = filledPrice;
        FilledAt = DateTime.UtcNow;
    }

    /// <summary>
    /// Marks this order as rejected, stopping further processing.
    /// </summary>
    public void MarkRejected()
    {
        Status = OrderStatus.Rejected;
    }

    /// <summary>
    /// Marks this order as cancelled by the tenant or system.
    /// </summary>
    public void MarkCancelled()
    {
        if (Status is OrderStatus.Filled or OrderStatus.Rejected)
        {
            throw new InvalidOperationException(
                $"Cannot cancel order {Id} in terminal status {Status}.");
        }

        Status = OrderStatus.Cancelled;
    }
}
