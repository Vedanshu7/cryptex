using TradingPlatform.OMS.Domain.Interfaces;

namespace TradingPlatform.OMS.Infrastructure.Services;

/// <summary>
/// Minimal risk service that enforces basic sanity checks.
/// Replace with position-limit and drawdown checks for production.
/// </summary>
public sealed class BasicRiskService : IRiskService
{
    private const decimal _maxQuantity = 100m;

    /// <inheritdoc/>
    public Task<RiskResult> ValidateAsync(
        Guid tenantId,
        string symbol,
        string side,
        decimal quantity,
        CancellationToken ct = default)
    {
        if (quantity <= 0)
        {
            return Task.FromResult(RiskResult.Fail("Quantity must be positive."));
        }

        if (quantity > _maxQuantity)
        {
            return Task.FromResult(
                RiskResult.Fail($"Quantity {quantity} exceeds maximum {_maxQuantity}."));
        }

        return Task.FromResult(RiskResult.Pass());
    }
}
