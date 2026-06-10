using TradingPlatform.EMS.Domain.Entities;

namespace TradingPlatform.EMS.Domain.Interfaces;

/// <summary>Persistence contract for execution records.</summary>
public interface IExecutionRepository
{
    /// <summary>Persists a new execution record.</summary>
    Task SaveAsync(Execution execution, CancellationToken ct = default);
}
