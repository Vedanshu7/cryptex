namespace TradingPlatform.OMS.Domain.Exceptions;

/// <summary>
/// Thrown when an order is rejected because the same signal has already been
/// processed for this tenant. Guards against Kafka at-least-once redelivery.
/// </summary>
public sealed class DuplicateSignalException : Exception
{
    /// <summary>Gets the signal that was already processed.</summary>
    public string SignalId { get; } = string.Empty;

    /// <summary>Gets the tenant for which the duplicate was detected.</summary>
    public Guid TenantId { get; }

    /// <summary>Required by CA1032.</summary>
    public DuplicateSignalException() { }

    /// <summary>Required by CA1032.</summary>
    public DuplicateSignalException(string message) : base(message) { }

    /// <summary>Required by CA1032.</summary>
    public DuplicateSignalException(string message, Exception innerException)
        : base(message, innerException) { }

    /// <summary>Initializes the exception with signal context.</summary>
    public DuplicateSignalException(string signalId, Guid tenantId)
        : base($"Signal '{signalId}' already processed for tenant {tenantId}.")
    {
        SignalId = signalId;
        TenantId = tenantId;
    }
}
