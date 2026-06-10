namespace TradingPlatform.EMS.Domain.Entities;

/// <summary>
/// Records the result of submitting an order to the exchange.
/// Immutable after creation — execution results are append-only.
/// </summary>
public sealed class Execution
{
    /// <summary>Gets the unique execution record identifier.</summary>
    public Guid Id { get; private set; }

    /// <summary>Gets the OMS order ID that triggered this execution.</summary>
    public Guid OrderId { get; private set; }

    /// <summary>Gets the tenant that owns this execution.</summary>
    public Guid TenantId { get; private set; }

    /// <summary>Gets the Binance-assigned order ID, if available.</summary>
    public long? ExchangeOrderId { get; private set; }

    /// <summary>Gets the actual fill price returned by the exchange.</summary>
    public decimal FillPrice { get; private set; }

    /// <summary>Gets the execution outcome.</summary>
    public ExecutionStatus Status { get; private set; }

    /// <summary>Gets the error message when Status is Error or Rejected.</summary>
    public string? ErrorMessage { get; private set; }

    /// <summary>Gets the UTC timestamp of this execution.</summary>
    public DateTime ExecutedAt { get; private set; }

    private Execution() { }

    /// <summary>Creates a successful fill record.</summary>
    public static Execution CreateFill(
        Guid orderId,
        Guid tenantId,
        long exchangeOrderId,
        decimal fillPrice)
    {
        return new Execution
        {
            Id              = Guid.NewGuid(),
            OrderId         = orderId,
            TenantId        = tenantId,
            ExchangeOrderId = exchangeOrderId,
            FillPrice       = fillPrice,
            Status          = ExecutionStatus.Filled,
            ExecutedAt      = DateTime.UtcNow,
        };
    }

    /// <summary>Creates a failed execution record.</summary>
    public static Execution CreateError(
        Guid orderId,
        Guid tenantId,
        string errorMessage)
    {
        return new Execution
        {
            Id           = Guid.NewGuid(),
            OrderId      = orderId,
            TenantId     = tenantId,
            Status       = ExecutionStatus.Error,
            ErrorMessage = errorMessage,
            ExecutedAt   = DateTime.UtcNow,
        };
    }
}
