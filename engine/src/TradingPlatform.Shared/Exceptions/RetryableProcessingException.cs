namespace TradingPlatform.Common.Exceptions;

/// <summary>
/// Thrown by an infrastructure boundary (database, exchange REST) to signal a
/// transient failure that a Kafka consumer's <c>RetryingDlqDispatcher</c>
/// should retry with backoff before quarantining the message to its DLQ.
/// </summary>
public sealed class RetryableProcessingException : Exception
{
    /// <summary>Required by CA1032.</summary>
    public RetryableProcessingException() { }

    /// <summary>Required by CA1032.</summary>
    public RetryableProcessingException(string message) : base(message) { }

    /// <summary>Required by CA1032.</summary>
    public RetryableProcessingException(string message, Exception innerException)
        : base(message, innerException) { }
}
