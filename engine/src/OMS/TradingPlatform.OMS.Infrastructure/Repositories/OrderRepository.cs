using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using TradingPlatform.OMS.Domain.Entities;
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
            throw;
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
        _context.Orders.Add(order);
        await _context.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task UpdateAsync(Order order, CancellationToken ct = default)
    {
        _context.Orders.Update(order);
        await _context.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Database error fetching order {OrderId}.")]
    private static partial void LogQueryError(ILogger logger, Guid orderId, Exception ex);
}
