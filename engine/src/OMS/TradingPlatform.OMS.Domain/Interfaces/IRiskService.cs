namespace TradingPlatform.OMS.Domain.Interfaces;

/// <summary>Evaluates whether an order request passes risk controls.</summary>
public interface IRiskService
{
    /// <summary>
    /// Validates the order parameters against risk limits.
    /// </summary>
    /// <param name="tenantId">Tenant placing the order.</param>
    /// <param name="symbol">Trading symbol.</param>
    /// <param name="side">BUY or SELL.</param>
    /// <param name="quantity">Requested quantity.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A <see cref="RiskResult"/> indicating pass or fail with a reason.</returns>
    Task<RiskResult> ValidateAsync(
        Guid tenantId,
        string symbol,
        string side,
        decimal quantity,
        CancellationToken ct = default);
}

/// <summary>Result of a risk validation check.</summary>
public sealed record RiskResult
{
    /// <summary>Gets whether the order passed all risk checks.</summary>
    public required bool Passed { get; init; }

    /// <summary>Gets the human-readable rejection reason when <see cref="Passed"/> is false.</summary>
    public string? Reason { get; init; }

    /// <summary>Creates a passing result.</summary>
    public static RiskResult Pass() => new() { Passed = true };

    /// <summary>Creates a failing result with the given reason.</summary>
    public static RiskResult Fail(string reason) => new() { Passed = false, Reason = reason };
}
