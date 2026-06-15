using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Domain.Interfaces;

/// <summary>Persistence contract for the Order aggregate.</summary>
public interface IOrderRepository
{
    /// <summary>Finds an order by its unique identifier.</summary>
    Task<Order?> GetByIdAsync(Guid id, CancellationToken ct = default);

    /// <summary>Returns all orders for a given tenant, newest first.</summary>
    Task<IReadOnlyList<Order>> GetByTenantAsync(Guid tenantId, CancellationToken ct = default);

    /// <summary>Persists a newly created order.</summary>
    Task SaveAsync(Order order, CancellationToken ct = default);

    /// <summary>Updates order status and optional fill details.</summary>
    Task UpdateAsync(Order order, CancellationToken ct = default);

    /// <summary>
    /// Returns all orders in Pending or Validated status older than
    /// <paramref name="olderThan"/>, across every tenant.
    /// Intended for the reconciliation service only — bypasses the tenant query filter.
    /// </summary>
    Task<IReadOnlyList<Order>> GetStuckAsync(TimeSpan olderThan, CancellationToken ct = default);
}
