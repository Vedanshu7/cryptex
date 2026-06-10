using MediatR;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Application.Queries;

/// <summary>Query to retrieve all positions for a given tenant.</summary>
public sealed record GetPositionsByTenantQuery : IRequest<IReadOnlyList<Position>>
{
    /// <summary>Gets the tenant whose positions to fetch.</summary>
    public required Guid TenantId { get; init; }
}
