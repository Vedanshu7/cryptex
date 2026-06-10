using System.Text.Json;
using Confluent.Kafka;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TradingPlatform.Common.Kafka;
using TradingPlatform.EMS.Application.Commands;

namespace TradingPlatform.EMS.Infrastructure.Kafka;

/// <summary>
/// Background service that consumes validated-orders from Kafka
/// and dispatches <see cref="ExecuteOrderCommand"/> via MediatR.
/// </summary>
public sealed partial class EmsKafkaConsumerService : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<EmsKafkaConsumerService> _logger;
    private readonly IConsumer<string, string> _consumer;

    /// <summary>Initializes the background service.</summary>
    public EmsKafkaConsumerService(
        IServiceScopeFactory scopeFactory,
        ILogger<EmsKafkaConsumerService> logger,
        IKafkaConsumerFactory consumerFactory)
    {
        ArgumentNullException.ThrowIfNull(consumerFactory);
        _scopeFactory = scopeFactory;
        _logger       = logger;
        _consumer     = consumerFactory.Create("ems-consumer", ["validated-orders"]);
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

                        await _ProcessMessageAsync(result.Message.Value, stoppingToken)
                            .ConfigureAwait(false);
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
            messageValue, JsonOptions);

        if (command is null)
        {
            LogDeserializationFailed(_logger, messageValue[..Math.Min(100, messageValue.Length)]);
            return;
        }

        await mediator.Send(command, cancellationToken).ConfigureAwait(false);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "EMS Kafka consumer started.")]
    private static partial void LogStarted(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "EMS Kafka consumer stopped.")]
    private static partial void LogStopped(ILogger logger);

    [LoggerMessage(Level = LogLevel.Error, Message = "EMS Kafka consumer error.")]
    private static partial void LogError(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to deserialize validated-orders message: {Snippet}.")]
    private static partial void LogDeserializationFailed(ILogger logger, string snippet);
}
