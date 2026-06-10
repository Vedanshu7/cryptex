using MediatR;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Application.Commands;

/// <summary>
/// Command to place a new trade order after signal routing.
/// Deserialised directly from the order-requests Kafka message.
/// </summary>
public sealed record PlaceOrderCommand : IRequest<PlaceOrderResult>
{
    /// <summary>Gets the tenant placing the order.</summary>
    public required Guid TenantId { get; init; }

    /// <summary>Gets the trading symbol (e.g. BTCUSDT).</summary>
    public required string Symbol { get; init; }

    /// <summary>Gets the order direction (BUY or SELL).</summary>
    public required string Side { get; init; }

    /// <summary>Gets the requested quantity.</summary>
    public required decimal Quantity { get; init; }

    /// <summary>Gets the originating signal identifier.</summary>
    public required string SignalId { get; init; }
}

/// <summary>Result returned by <see cref="PlaceOrderCommand"/>.</summary>
public sealed record PlaceOrderResult
{
    /// <summary>Gets the created order ID, or <see cref="Guid.Empty"/> if rejected.</summary>
    public required Guid OrderId { get; init; }

    /// <summary>Gets the resulting order status.</summary>
    public required OrderStatus Status { get; init; }

    /// <summary>Gets the rejection reason when <see cref="Status"/> is Rejected.</summary>
    public string? RejectionReason { get; init; }
}
