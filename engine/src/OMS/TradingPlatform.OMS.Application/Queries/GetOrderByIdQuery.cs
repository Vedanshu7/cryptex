using MediatR;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Application.Queries;

/// <summary>Query to retrieve a single order by its ID.</summary>
public sealed record GetOrderByIdQuery : IRequest<Order?>
{
    /// <summary>Gets the order identifier to look up.</summary>
    public required Guid OrderId { get; init; }

    /// <summary>Gets the requesting tenant (used for RLS scoping).</summary>
    public required Guid TenantId { get; init; }
}
