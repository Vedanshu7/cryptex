using MediatR;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Application.Commands;

/// <summary>
/// Command to update an order's status when a fill arrives from the EMS.
/// Consumed from the order-fills Kafka topic.
/// </summary>
public sealed record UpdateOrderStatusCommand : IRequest<Unit>
{
    /// <summary>Gets the order to update.</summary>
    public required Guid OrderId { get; init; }

    /// <summary>Gets the tenant ID (used to scope the DB query via RLS).</summary>
    public required Guid TenantId { get; init; }

    /// <summary>Gets the new status to apply.</summary>
    public required OrderStatus NewStatus { get; init; }

    /// <summary>Gets the fill price when status is Filled or PartialFilled.</summary>
    public decimal? FillPrice { get; init; }
}
