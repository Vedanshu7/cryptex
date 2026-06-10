using MediatR;
using Microsoft.Extensions.Logging;
using TradingPlatform.Common.Kafka;
using TradingPlatform.OMS.Application.Commands;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Interfaces;

namespace TradingPlatform.OMS.Application.Handlers;

/// <summary>
/// Handles <see cref="PlaceOrderCommand"/> — runs risk checks, persists the order,
/// and publishes to the validated-orders topic for EMS to execute.
/// </summary>
public sealed partial class PlaceOrderHandler
    : IRequestHandler<PlaceOrderCommand, PlaceOrderResult>
{
    private readonly IOrderRepository _orderRepository;
    private readonly IRiskService _riskService;
    private readonly IKafkaProducer _kafkaProducer;
    private readonly ILogger<PlaceOrderHandler> _logger;

    /// <summary>Initializes handler with its dependencies.</summary>
    public PlaceOrderHandler(
        IOrderRepository orderRepository,
        IRiskService riskService,
        IKafkaProducer kafkaProducer,
        ILogger<PlaceOrderHandler> logger)
    {
        _orderRepository = orderRepository;
        _riskService     = riskService;
        _kafkaProducer   = kafkaProducer;
        _logger          = logger;
    }

    /// <inheritdoc/>
    public async Task<PlaceOrderResult> Handle(
        PlaceOrderCommand request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        RiskResult risk = await _riskService
            .ValidateAsync(
                request.TenantId,
                request.Symbol,
                request.Side,
                request.Quantity,
                cancellationToken)
            .ConfigureAwait(false);

        if (!risk.Passed)
        {
            LogRiskFailed(_logger, request.TenantId, risk.Reason ?? "unknown");

            return new PlaceOrderResult
            {
                OrderId         = Guid.Empty,
                Status          = OrderStatus.Rejected,
                RejectionReason = risk.Reason,
            };
        }

        Order order = Order.Create(
            request.TenantId,
            request.Symbol,
            request.Side,
            request.Quantity,
            request.SignalId);

        order.MarkValidated();

        await _orderRepository.SaveAsync(order, cancellationToken).ConfigureAwait(false);

        await _kafkaProducer
            .PublishAsync(
                topic:  "validated-orders",
                key:    request.TenantId.ToString(),
                value:  order,
                cancellationToken)
            .ConfigureAwait(false);

        LogOrderCreated(_logger, order.Id, request.TenantId);

        return new PlaceOrderResult
        {
            OrderId = order.Id,
            Status  = OrderStatus.Validated,
        };
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Risk check failed for tenant {TenantId}: {Reason}.")]
    private static partial void LogRiskFailed(ILogger logger, Guid tenantId, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "Order {OrderId} validated for tenant {TenantId}.")]
    private static partial void LogOrderCreated(ILogger logger, Guid orderId, Guid tenantId);
}
