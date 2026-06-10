using MediatR;
using TradingPlatform.OMS.Application.Queries;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Interfaces;

namespace TradingPlatform.OMS.Application.Handlers;

/// <summary>Handles <see cref="GetPositionsByTenantQuery"/>.</summary>
public sealed class GetPositionsByTenantHandler
    : IRequestHandler<GetPositionsByTenantQuery, IReadOnlyList<Position>>
{
    private readonly IPositionRepository _positionRepository;

    /// <summary>Initializes the handler.</summary>
    public GetPositionsByTenantHandler(IPositionRepository positionRepository)
    {
        _positionRepository = positionRepository;
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<Position>> Handle(
        GetPositionsByTenantQuery request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Fetch all symbols the tenant has an open position for.
        // The global EF query filter ensures tenant isolation automatically.
        return await _positionRepository
            .GetByTenantAsync(request.TenantId, cancellationToken)
            .ConfigureAwait(false);
    }
}
