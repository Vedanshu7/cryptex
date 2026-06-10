using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Domain.Interfaces;

/// <summary>Persistence contract for the Position aggregate.</summary>
public interface IPositionRepository
{
    /// <summary>Returns a tenant's position for a given symbol, or null if none exists.</summary>
    Task<Position?> GetAsync(Guid tenantId, string symbol, CancellationToken ct = default);

    /// <summary>Returns all positions for a given tenant.</summary>
    Task<IReadOnlyList<Position>> GetByTenantAsync(Guid tenantId, CancellationToken ct = default);

    /// <summary>Persists a new or updated position.</summary>
    Task SaveAsync(Position position, CancellationToken ct = default);
}
