using System.Text.Json;
using Confluent.Kafka;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using TradingPlatform.Common.Kafka;
using TradingPlatform.OMS.Application.Commands;
using TradingPlatform.OMS.Infrastructure.Persistence;

namespace TradingPlatform.OMS.Infrastructure.Kafka;

/// <summary>
/// Background service that continuously consumes from the order-requests topic
/// and dispatches <see cref="PlaceOrderCommand"/> via MediatR.
/// Runs for the lifetime of the application.
/// </summary>
public sealed partial class KafkaConsumerService : BackgroundService
{
    // Cached to satisfy CA1869 — JsonSerializerOptions are expensive to construct.
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly IConsumer<string, string> _consumer;

    /// <summary>Initializes the background service.</summary>
    public KafkaConsumerService(
        IServiceScopeFactory scopeFactory,
        ILogger<KafkaConsumerService> logger,
        IKafkaConsumerFactory consumerFactory)
    {
        ArgumentNullException.ThrowIfNull(consumerFactory);
        _scopeFactory = scopeFactory;
        _logger       = logger;
        _consumer     = consumerFactory.Create("oms-consumer", ["order-requests"]);
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
                        LogProcessingError(_logger, ex);
                        await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken)
                            .ConfigureAwait(false);
                    }
                    catch (KafkaException ex)
                    {
                        LogProcessingError(_logger, ex);
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
        // Each message gets a fresh scope so EF Core DbContext is not shared.
        using IServiceScope scope = _scopeFactory.CreateScope();
        IMediator mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

        PlaceOrderCommand? command = JsonSerializer.Deserialize<PlaceOrderCommand>(
            messageValue,
            JsonOptions);

        if (command is null)
        {
            LogDeserializationFailed(_logger, messageValue[..Math.Min(100, messageValue.Length)]);
            return;
        }

        // Populate tenant context so TenantDbCommandInterceptor injects
        // app.current_tenant_id into every EF Core command — required for RLS.
        // Kafka messages bypass HTTP middleware, so we set it manually here.
        TenantContext tenantContext =
            scope.ServiceProvider.GetRequiredService<TenantContext>();
        tenantContext.TenantId = command.TenantId;

        await mediator.Send(command, cancellationToken).ConfigureAwait(false);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "OMS Kafka consumer started.")]
    private static partial void LogStarted(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "OMS Kafka consumer stopped.")]
    private static partial void LogStopped(ILogger logger);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error processing Kafka message in OMS.")]
    private static partial void LogProcessingError(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to deserialize order-requests message: {Snippet}.")]
    private static partial void LogDeserializationFailed(ILogger logger, string snippet);
}
