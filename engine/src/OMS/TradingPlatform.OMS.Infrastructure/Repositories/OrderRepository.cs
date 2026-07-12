using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Npgsql;
using TradingPlatform.Common.Exceptions;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Exceptions;
using TradingPlatform.OMS.Domain.Interfaces;
using TradingPlatform.OMS.Infrastructure.Persistence;

namespace TradingPlatform.OMS.Infrastructure.Repositories;

/// <summary>
/// PostgreSQL implementation of <see cref="IOrderRepository"/> using EF Core.
/// </summary>
public sealed partial class OrderRepository : IOrderRepository
{
    private readonly TradingDbContext _context;
    private readonly ILogger<OrderRepository> _logger;

    /// <summary>Initializes the repository.</summary>
    public OrderRepository(TradingDbContext context, ILogger<OrderRepository> logger)
    {
        _context = context;
        _logger  = logger;
    }

    /// <inheritdoc/>
    public async Task<Order?> GetByIdAsync(Guid id, CancellationToken ct = default)
    {
        try
        {
            return await _context.Orders
                .FirstOrDefaultAsync(o => o.Id == id, ct)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (DbUpdateException ex)
        {
            LogQueryError(_logger, id, ex);
            throw new RetryableProcessingException($"Failed to fetch order {id}.", ex);
        }
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<Order>> GetByTenantAsync(
        Guid tenantId,
        CancellationToken ct = default)
    {
        return await _context.Orders
            .Where(o => o.TenantId == tenantId)
            .OrderByDescending(o => o.CreatedAt)
            .ToListAsync(ct)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task SaveAsync(Order order, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(order);

        _context.Orders.Add(order);
        try
        {
            await _context.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch (DbUpdateException ex)
            when (ex.InnerException is PostgresException pg && pg.SqlState == "23505")
        {
            // uq_orders_tenant_signal fired — same signal redelivered by Kafka.
            throw new DuplicateSignalException(order.SignalId ?? string.Empty, order.TenantId);
        }
        catch (DbUpdateException ex)
        {
            throw new RetryableProcessingException($"Failed to save order {order.Id}.", ex);
        }
    }

    /// <inheritdoc/>
    public async Task UpdateAsync(Order order, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(order);

        _context.Orders.Update(order);
        try
        {
            await _context.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch (DbUpdateException ex)
        {
            throw new RetryableProcessingException($"Failed to update order {order.Id}.", ex);
        }
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<Order>> GetStuckAsync(
        TimeSpan olderThan,
        CancellationToken ct = default)
    {
        DateTime cutoff = DateTime.UtcNow - olderThan;

        // Open an explicit transaction so SET LOCAL applies to the LINQ query on the
        // same connection. The system_bypass RLS policy (migration 009) allows this
        // cross-tenant scan when app.is_system_operation = 'true' is set.
        Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction tx =
            await _context.Database.BeginTransactionAsync(ct).ConfigureAwait(false);

        try
        {
            await _context.Database
                .ExecuteSqlRawAsync("SET LOCAL app.is_system_operation = 'true'", ct)
                .ConfigureAwait(false);

            IReadOnlyList<Order> stuck = await _context.Orders
                .IgnoreQueryFilters()
                .Where(o => (o.Status == OrderStatus.Pending || o.Status == OrderStatus.Validated)
                            && o.CreatedAt < cutoff)
                .ToListAsync(ct)
                .ConfigureAwait(false);

            await tx.CommitAsync(ct).ConfigureAwait(false);
            return stuck;
        }
        finally
        {
            await tx.DisposeAsync().ConfigureAwait(false);
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Database error fetching order {OrderId}.")]
    private static partial void LogQueryError(ILogger logger, Guid orderId, Exception ex);
}
