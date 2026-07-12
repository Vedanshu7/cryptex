using System.Text.Json;
using Confluent.Kafka;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TradingPlatform.Common.Exceptions;
using TradingPlatform.Common.Kafka;
using TradingPlatform.OMS.Application.Commands;
using TradingPlatform.OMS.Application.Settings;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Infrastructure.Persistence;

namespace TradingPlatform.OMS.Infrastructure.Kafka;

/// <summary>
/// Background service that consumes fill events from Kafka and dispatches
/// <see cref="UpdateOrderStatusCommand"/> to update order status and positions.
/// The topic is set via OMS__Topics__FillsTopic — each regional OMS instance
/// reads from its own fills topic (e.g. tokyo.order-fills).
/// </summary>
public sealed partial class OrderFillsConsumerService : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OrderFillsConsumerService> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly IRetryingDlqDispatcher _dispatcher;
    private readonly string _fillsTopic;

    /// <summary>Initializes the background service.</summary>
    public OrderFillsConsumerService(
        IServiceScopeFactory scopeFactory,
        ILogger<OrderFillsConsumerService> logger,
        IKafkaConsumerFactory consumerFactory,
        IRetryingDlqDispatcher dispatcher,
        IOptions<OmsTopicSettings> topics)
    {
        ArgumentNullException.ThrowIfNull(consumerFactory);
        ArgumentNullException.ThrowIfNull(topics);
        _scopeFactory = scopeFactory;
        _logger       = logger;
        _dispatcher   = dispatcher;
        _fillsTopic   = topics.Value.FillsTopic;
        _consumer = consumerFactory.Create($"oms-fills-{_fillsTopic}", [_fillsTopic]);
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
                            _fillsTopic,
                            result.Message.Key,
                            result.Message.Value,
                            ct => _ProcessFillAsync(result.Message.Value, ct),
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

    private async Task _ProcessFillAsync(
        string messageValue,
        CancellationToken cancellationToken)
    {
        OrderFillEvent? fillEvent = JsonSerializer.Deserialize<OrderFillEvent>(
            messageValue, JsonOptions);

        if (fillEvent is null)
        {
            throw new NonRetryableProcessingException(
                $"Deserialized OrderFillEvent was null: {messageValue[..Math.Min(100, messageValue.Length)]}.");
        }

        using IServiceScope scope = _scopeFactory.CreateScope();

        // Set tenant context so RLS applies to the DB update.
        TenantContext tenantContext = scope.ServiceProvider.GetRequiredService<TenantContext>();
        tenantContext.TenantId = fillEvent.TenantId;

        IMediator mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

        await mediator
            .Send(
                new UpdateOrderStatusCommand
                {
                    OrderId   = fillEvent.OrderId,
                    TenantId  = fillEvent.TenantId,
                    NewStatus = OrderStatus.Filled,
                    FillPrice = fillEvent.FillPrice,
                },
                cancellationToken)
            .ConfigureAwait(false);

        LogFillProcessed(_logger, fillEvent.OrderId, fillEvent.FillPrice);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "OMS order-fills consumer started.")]
    private static partial void LogStarted(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "OMS order-fills consumer stopped.")]
    private static partial void LogStopped(ILogger logger);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in OMS order-fills consumer.")]
    private static partial void LogError(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Order {OrderId} fill processed at {FillPrice}.")]
    private static partial void LogFillProcessed(ILogger logger, Guid orderId, decimal fillPrice);
}
