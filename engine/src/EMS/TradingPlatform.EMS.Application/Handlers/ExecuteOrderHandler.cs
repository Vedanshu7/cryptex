using MediatR;
using Microsoft.Extensions.Logging;
using TradingPlatform.Common.Kafka;
using TradingPlatform.EMS.Application.Commands;
using TradingPlatform.EMS.Domain.Entities;
using TradingPlatform.EMS.Domain.Interfaces;

namespace TradingPlatform.EMS.Application.Handlers;

/// <summary>
/// Handles <see cref="ExecuteOrderCommand"/> — submits to Binance, persists the
/// execution record, and publishes a fill event back to the OMS via Kafka.
/// </summary>
public sealed partial class ExecuteOrderHandler
    : IRequestHandler<ExecuteOrderCommand, ExecuteOrderResult>
{
    private readonly IBinanceClient _binanceClient;
    private readonly IExecutionRepository _executionRepository;
    private readonly IKafkaProducer _kafkaProducer;
    private readonly ILogger<ExecuteOrderHandler> _logger;

    /// <summary>Initializes the handler with its dependencies.</summary>
    public ExecuteOrderHandler(
        IBinanceClient binanceClient,
        IExecutionRepository executionRepository,
        IKafkaProducer kafkaProducer,
        ILogger<ExecuteOrderHandler> logger)
    {
        _binanceClient       = binanceClient;
        _executionRepository = executionRepository;
        _kafkaProducer       = kafkaProducer;
        _logger              = logger;
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
                    topic: "order-fills",
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
