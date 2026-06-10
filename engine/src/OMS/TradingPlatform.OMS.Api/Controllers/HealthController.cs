using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TradingPlatform.OMS.Infrastructure.Persistence;

namespace TradingPlatform.OMS.Api.Controllers;

/// <summary>
/// Liveness and readiness probe endpoint for Docker/Kubernetes.
/// </summary>
[ApiController]
[Route("health")]
public sealed class HealthController : ControllerBase
{
    private readonly TradingDbContext _dbContext;

    /// <summary>Initializes the controller.</summary>
    public HealthController(TradingDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    /// <summary>
    /// Returns HTTP 200 with connectivity status for each dependency.
    /// Returns HTTP 503 if any critical dependency is unavailable.
    /// </summary>
    [HttpGet]
    public async Task<IActionResult> GetAsync(CancellationToken ct)
    {
        bool dbOk = await _CheckDatabaseAsync(ct).ConfigureAwait(false);

        object status = new
        {
            db     = dbOk ? "ok" : "error",
            status = dbOk ? "healthy" : "degraded",
        };

        return dbOk ? Ok(status) : StatusCode(503, status);
    }

    private async Task<bool> _CheckDatabaseAsync(CancellationToken ct)
    {
        try
        {
            await _dbContext.Database.ExecuteSqlRawAsync("SELECT 1", ct)
                .ConfigureAwait(false);
            return true;
        }
        catch (DbUpdateException)
        {
            return false;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
        catch (Npgsql.NpgsqlException)
        {
            return false;
        }
    }
}
