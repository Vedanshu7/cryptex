namespace TradingPlatform.OMS.Infrastructure.Persistence;

/// <summary>
/// Provides the current tenant identifier for RLS session variable injection.
/// Scoped per HTTP request / Kafka message scope.
/// </summary>
public interface ITenantContext
{
    /// <summary>Gets the current tenant's UUID, or null for system-level operations.</summary>
    Guid? TenantId { get; }
}

/// <summary>Mutable tenant context populated by middleware or Kafka consumer.</summary>
public sealed class TenantContext : ITenantContext
{
    /// <inheritdoc/>
    public Guid? TenantId { get; set; }
}
