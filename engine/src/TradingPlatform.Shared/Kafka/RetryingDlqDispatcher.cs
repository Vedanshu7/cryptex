using System.Text.Json;
using Microsoft.Extensions.Logging;
using TradingPlatform.Common.Exceptions;

namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Default <see cref="IRetryingDlqDispatcher"/>: fixed-count retry with linear
/// backoff, backed by an <see cref="IDeadLetterPublisher"/>. Shared by every
/// Kafka consumer (EMS and OMS alike) so retry/DLQ behavior is defined once.
/// </summary>
public sealed partial class RetryingDlqDispatcher : IRetryingDlqDispatcher
{
    private const int MaxAttempts = 3;
    private static readonly TimeSpan BaseDelay = TimeSpan.FromMilliseconds(500);

    private readonly IDeadLetterPublisher _dlq;
    private readonly ILogger<RetryingDlqDispatcher> _logger;
    private readonly string _serviceName;

    /// <summary>Initializes the dispatcher with its dead-letter publisher.</summary>
    /// <param name="dlq">Publisher used to quarantine messages this dispatcher can't process.</param>
    /// <param name="logger">Logger for retry/quarantine events.</param>
    /// <param name="serviceName">
    /// "service" label value for the pipeline_dlq_messages_total /
    /// pipeline_retry_attempts_total counters (e.g. "oms", "ems") — set via
    /// DI registration in each API's Program.cs.
    /// </param>
    public RetryingDlqDispatcher(
        IDeadLetterPublisher dlq, ILogger<RetryingDlqDispatcher> logger, string serviceName)
    {
        ArgumentNullException.ThrowIfNull(dlq);
        ArgumentException.ThrowIfNullOrEmpty(serviceName);
        _dlq         = dlq;
        _logger      = logger;
        _serviceName = serviceName;
    }

    /// <inheritdoc/>
    public async Task DispatchAsync(
        string sourceTopic,
        string? key,
        string rawValue,
        Func<CancellationToken, Task> processOnce,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(processOnce);

        for (int attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            try
            {
                await processOnce(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (JsonException ex)
            {
                LogQuarantining(_logger, sourceTopic, ex.GetType().Name, attempt);
                DlqMetrics.RecordDlqMessage(_serviceName, sourceTopic, ex.GetType().Name);
                await _dlq.PublishAsync(sourceTopic, key, rawValue, ex, attempt, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }
            catch (NonRetryableProcessingException ex)
            {
                LogQuarantining(_logger, sourceTopic, ex.GetType().Name, attempt);
                DlqMetrics.RecordDlqMessage(_serviceName, sourceTopic, ex.GetType().Name);
                await _dlq.PublishAsync(sourceTopic, key, rawValue, ex, attempt, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }
            catch (RetryableProcessingException ex) when (attempt < MaxAttempts)
            {
                LogRetrying(_logger, sourceTopic, attempt, ex);
                DlqMetrics.RecordRetryAttempt(_serviceName, sourceTopic);
                await Task.Delay(BaseDelay * attempt, cancellationToken).ConfigureAwait(false);
            }
            catch (RetryableProcessingException ex)
            {
                LogQuarantining(_logger, sourceTopic, ex.GetType().Name, attempt);
                DlqMetrics.RecordDlqMessage(_serviceName, sourceTopic, ex.GetType().Name);
                await _dlq.PublishAsync(sourceTopic, key, rawValue, ex, attempt, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Retrying {SourceTopic} after attempt {Attempt}.")]
    private static partial void LogRetrying(ILogger logger, string sourceTopic, int attempt, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Quarantining message from {SourceTopic} to DLQ after {Attempt} attempt(s): {ErrorType}.")]
    private static partial void LogQuarantining(ILogger logger, string sourceTopic, string errorType, int attempt);
}
