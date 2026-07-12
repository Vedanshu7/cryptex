namespace TradingPlatform.Common.Exceptions;

/// <summary>
/// Thrown to signal a message-processing failure that retrying would not fix
/// (validation, a rejected business rule, a malformed upstream response) — a
/// Kafka consumer's <c>RetryingDlqDispatcher</c> routes this straight to the
/// DLQ with no retry attempts.
/// </summary>
public sealed class NonRetryableProcessingException : Exception
{
    /// <summary>Required by CA1032.</summary>
    public NonRetryableProcessingException() { }

    /// <summary>Required by CA1032.</summary>
    public NonRetryableProcessingException(string message) : base(message) { }

    /// <summary>Required by CA1032.</summary>
    public NonRetryableProcessingException(string message, Exception innerException)
        : base(message, innerException) { }
}
