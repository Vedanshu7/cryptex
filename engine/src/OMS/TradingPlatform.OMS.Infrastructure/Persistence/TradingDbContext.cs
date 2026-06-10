using Microsoft.EntityFrameworkCore;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Infrastructure.Persistence;

/// <summary>
/// EF Core DbContext for the trading platform OMS.
/// Connects to PostgreSQL with TimescaleDB extension.
///
/// Global query filters enforce tenant isolation at the LINQ layer as a
/// second line of defence — the first being the SQL-level RLS policies
/// injected by <see cref="TenantDbCommandInterceptor"/>.
/// </summary>
public sealed class TradingDbContext : DbContext
{
    private readonly ITenantContext _tenantContext;

    /// <summary>Gets the orders dataset.</summary>
    public DbSet<Order> Orders => Set<Order>();

    /// <summary>Gets the positions dataset.</summary>
    public DbSet<Position> Positions => Set<Position>();

    /// <summary>Initializes the context with options and tenant context.</summary>
    public TradingDbContext(
        DbContextOptions<TradingDbContext> options,
        ITenantContext tenantContext)
        : base(options)
    {
        _tenantContext = tenantContext;
    }

    /// <inheritdoc/>
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.ApplyConfigurationsFromAssembly(typeof(TradingDbContext).Assembly);

        // Global query filters: lambdas close over the *instance field* _tenantContext,
        // not a captured local. OnModelCreating runs once and its result is cached, but
        // EF Core evaluates the filter against each DbContext instance at query time —
        // so _tenantContext.TenantId is read from the current scoped instance, not a
        // stale snapshot. Call .IgnoreQueryFilters() only for admin/system operations.
        modelBuilder.Entity<Order>()
            .HasQueryFilter(o => _tenantContext.TenantId == null
                || o.TenantId == _tenantContext.TenantId);
        modelBuilder.Entity<Position>()
            .HasQueryFilter(p => _tenantContext.TenantId == null
                || p.TenantId == _tenantContext.TenantId);

        base.OnModelCreating(modelBuilder);
    }
}
