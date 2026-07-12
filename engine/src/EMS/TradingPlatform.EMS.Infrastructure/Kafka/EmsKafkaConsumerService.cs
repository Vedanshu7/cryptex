using System.Text.Json;
using Confluent.Kafka;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TradingPlatform.Common.Exceptions;
using TradingPlatform.Common.Kafka;
using TradingPlatform.EMS.Application.Commands;
using TradingPlatform.EMS.Application.Settings;

namespace TradingPlatform.EMS.Infrastructure.Kafka;

/// <summary>
/// Background service that consumes validated orders from the configured topic
/// and dispatches <see cref="ExecuteOrderCommand"/> via MediatR.
/// The topic is set via EMS__Topics__InputTopic — each regional EMS instance
/// reads from its own topic (e.g. tokyo.validated-orders). Per-message
/// processing failures go through <see cref="IRetryingDlqDispatcher"/>, which
/// retries transient failures with backoff and quarantines anything it can't
/// recover from to "{InputTopic}.dlq" instead of crashing this service.
/// </summary>
public sealed partial class EmsKafkaConsumerService : BackgroundService
{
    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<EmsKafkaConsumerService> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly IRetryingDlqDispatcher _dispatcher;
    private readonly string _inputTopic;

    /// <summary>Initializes the background service.</summary>
    public EmsKafkaConsumerService(
        IServiceScopeFactory scopeFactory,
        ILogger<EmsKafkaConsumerService> logger,
        IKafkaConsumerFactory consumerFactory,
        IRetryingDlqDispatcher dispatcher,
        IOptions<EmsTopicSettings> topics)
    {
        ArgumentNullException.ThrowIfNull(consumerFactory);
        ArgumentNullException.ThrowIfNull(topics);
        _scopeFactory = scopeFactory;
        _logger       = logger;
        _dispatcher   = dispatcher;
        _inputTopic   = topics.Value.InputTopic;
        _consumer = consumerFactory.Create($"ems-{_inputTopic}", [_inputTopic]);
    }

    /// <inheritdoc/>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogStarted(_logger);

        await Task.Run(
            async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        ConsumeResult<string, string> result =
                            _consumer.Consume(stoppingToken);

                        if (result?.Message?.Value is null)
                        {
                            continue;
                        }

                        await _dispatcher.DispatchAsync(
                            _inputTopic,
                            result.Message.Key,
                            result.Message.Value,
                            ct => _ProcessMessageAsync(result.Message.Value, ct),
                            stoppingToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (ConsumeException ex)
                    {
                        LogError(_logger, ex);
                        await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken)
                            .ConfigureAwait(false);
                    }
                    catch (KafkaException ex)
                    {
                        LogError(_logger, ex);
                        await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken)
                            .ConfigureAwait(false);
                    }
                }
            },
            stoppingToken).ConfigureAwait(false);

        _consumer.Close();
        LogStopped(_logger);
    }

    /// <inheritdoc/>
    public override void Dispose()
    {
        _consumer.Dispose();
        base.Dispose();
    }

    private async Task _ProcessMessageAsync(
        string messageValue,
        CancellationToken cancellationToken)
    {
        using IServiceScope scope = _scopeFactory.CreateScope();
        IMediator mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

        ExecuteOrderCommand? command = JsonSerializer.Deserialize<ExecuteOrderCommand>(
            messageValue, _jsonOptions);

        if (command is null)
        {
            throw new NonRetryableProcessingException(
                $"Deserialized ExecuteOrderCommand was null: {messageValue[..Math.Min(100, messageValue.Length)]}.");
        }

        await mediator.Send(command, cancellationToken).ConfigureAwait(false);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "EMS Kafka consumer started.")]
    private static partial void LogStarted(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "EMS Kafka consumer stopped.")]
    private static partial void LogStopped(ILogger logger);

    [LoggerMessage(Level = LogLevel.Error, Message = "EMS Kafka consumer error.")]
    private static partial void LogError(ILogger logger, Exception ex);
}
