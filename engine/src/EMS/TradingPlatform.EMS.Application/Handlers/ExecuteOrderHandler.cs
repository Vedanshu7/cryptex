using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TradingPlatform.Common.Kafka;
using TradingPlatform.EMS.Application.Commands;
using TradingPlatform.EMS.Application.Settings;
using TradingPlatform.EMS.Domain.Entities;
using TradingPlatform.EMS.Domain.Interfaces;

namespace TradingPlatform.EMS.Application.Handlers;

/// <summary>
/// Handles <see cref="ExecuteOrderCommand"/> — submits to Binance, persists the
/// execution record, and publishes a fill event back to the OMS via Kafka.
/// The fills topic is set via EMS__Topics__OutputTopic so each regional EMS
/// publishes to its own fills topic (e.g. tokyo.order-fills).
/// </summary>
public sealed partial class ExecuteOrderHandler
    : IRequestHandler<ExecuteOrderCommand, ExecuteOrderResult>
{
    private readonly IBinanceClient _binanceClient;
    private readonly IExecutionRepository _executionRepository;
    private readonly IKafkaProducer _kafkaProducer;
    private readonly ILogger<ExecuteOrderHandler> _logger;
    private readonly EmsTopicSettings _topics;

    /// <summary>Initializes the handler with its dependencies.</summary>
    public ExecuteOrderHandler(
        IBinanceClient binanceClient,
        IExecutionRepository executionRepository,
        IKafkaProducer kafkaProducer,
        ILogger<ExecuteOrderHandler> logger,
        IOptions<EmsTopicSettings> topics)
    {
        ArgumentNullException.ThrowIfNull(topics);
        _binanceClient       = binanceClient;
        _executionRepository = executionRepository;
        _kafkaProducer       = kafkaProducer;
        _logger              = logger;
        _topics              = topics.Value;
    }

    /// <inheritdoc/>
    public async Task<ExecuteOrderResult> Handle(
        ExecuteOrderCommand request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        try
        {
            BinanceFillResult fill = await _binanceClient
                .PlaceMarketOrderAsync(
                    request.Symbol,
                    request.Side,
                    request.Quantity,
                    request.OrderId.ToString(),   // newClientOrderId → enables reconciliation lookup
                    cancellationToken)
                .ConfigureAwait(false);

            Execution execution = Execution.CreateFill(
                request.OrderId,
                request.TenantId,
                fill.OrderId,
                fill.FillPrice);

            await _executionRepository
                .SaveAsync(execution, cancellationToken)
                .ConfigureAwait(false);

            await _kafkaProducer
                .PublishAsync(
                    topic: _topics.OutputTopic,
                    key:   request.TenantId.ToString(),
                    value: new OrderFillEvent
                    {
                        OrderId         = request.OrderId,
                        TenantId        = request.TenantId,
                        FillPrice       = fill.FillPrice,
                        ExchangeOrderId = fill.OrderId,
                    },
                    cancellationToken)
                .ConfigureAwait(false);

            LogFilled(_logger, request.OrderId, fill.FillPrice);

            return new ExecuteOrderResult
            {
                Success         = true,
                ExchangeOrderId = fill.OrderId,
                FillPrice       = fill.FillPrice,
            };
        }
        catch (HttpRequestException ex)
        {
            return await _HandleErrorAsync(request, ex.Message, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (InvalidOperationException ex)
        {
            return await _HandleErrorAsync(request, ex.Message, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TaskCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            // A request timeout, not caller cancellation — treat the same as
            // the other known Binance-call failure modes above rather than
            // letting it crash the consumer.
            return await _HandleErrorAsync(request, ex.Message, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async Task<ExecuteOrderResult> _HandleErrorAsync(
        ExecuteOrderCommand request,
        string errorMessage,
        CancellationToken cancellationToken)
    {
        LogExecutionFailed(_logger, request.OrderId, errorMessage);

        Execution execution = Execution.CreateError(
            request.OrderId,
            request.TenantId,
            errorMessage);

        await _executionRepository
            .SaveAsync(execution, cancellationToken)
            .ConfigureAwait(false);

        return new ExecuteOrderResult
        {
            Success      = false,
            ErrorMessage = errorMessage,
        };
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Order {OrderId} filled at {FillPrice}.")]
    private static partial void LogFilled(ILogger logger, Guid orderId, decimal fillPrice);

    [LoggerMessage(Level = LogLevel.Error, Message = "Execution failed for order {OrderId}: {Error}.")]
    private static partial void LogExecutionFailed(ILogger logger, Guid orderId, string error);
}
