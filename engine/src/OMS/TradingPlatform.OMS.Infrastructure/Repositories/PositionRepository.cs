using Microsoft.EntityFrameworkCore;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Interfaces;
using TradingPlatform.OMS.Infrastructure.Persistence;

namespace TradingPlatform.OMS.Infrastructure.Repositories;

/// <summary>
/// PostgreSQL implementation of <see cref="IPositionRepository"/> using EF Core.
/// </summary>
public sealed class PositionRepository : IPositionRepository
{
    private readonly TradingDbContext _context;

    /// <summary>Initializes the repository.</summary>
    public PositionRepository(TradingDbContext context)
    {
        _context = context;
    }

    /// <inheritdoc/>
    public async Task<Position?> GetAsync(
        Guid tenantId,
        string symbol,
        CancellationToken ct = default)
    {
        return await _context.Positions
            .FirstOrDefaultAsync(
                p => p.TenantId == tenantId && p.Symbol == symbol,
                ct)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<Position>> GetByTenantAsync(
        Guid tenantId,
        CancellationToken ct = default)
    {
        // Global query filter already scopes to tenantId; the explicit Where is defence-in-depth.
        return await _context.Positions
            .Where(p => p.TenantId == tenantId)
            .ToListAsync(ct)
            .ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task SaveAsync(Position position, CancellationToken ct = default)
    {
        bool exists = await _context.Positions
            .AnyAsync(p => p.Id == position.Id, ct)
            .ConfigureAwait(false);

        if (exists)
        {
            _context.Positions.Update(position);
        }
        else
        {
            _context.Positions.Add(position);
        }

        await _context.SaveChangesAsync(ct).ConfigureAwait(false);
    }
}
