using MediatR;
using Microsoft.Extensions.Logging;
using TradingPlatform.OMS.Application.Commands;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Interfaces;

namespace TradingPlatform.OMS.Application.Handlers;

/// <summary>
/// Handles <see cref="UpdateOrderStatusCommand"/> — applies fill results
/// from the EMS and updates position data accordingly.
/// </summary>
public sealed partial class UpdateOrderStatusHandler
    : IRequestHandler<UpdateOrderStatusCommand, Unit>
{
    private readonly IOrderRepository _orderRepository;
    private readonly IPositionRepository _positionRepository;
    private readonly ILogger<UpdateOrderStatusHandler> _logger;

    /// <summary>Initializes the handler with its dependencies.</summary>
    public UpdateOrderStatusHandler(
        IOrderRepository orderRepository,
        IPositionRepository positionRepository,
        ILogger<UpdateOrderStatusHandler> logger)
    {
        _orderRepository    = orderRepository;
        _positionRepository = positionRepository;
        _logger             = logger;
    }

    /// <inheritdoc/>
    public async Task<Unit> Handle(
        UpdateOrderStatusCommand request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        Order? order = await _orderRepository
            .GetByIdAsync(request.OrderId, cancellationToken)
            .ConfigureAwait(false);

        if (order is null)
        {
            LogOrderNotFound(_logger, request.OrderId);
            return Unit.Value;
        }

        if (request.NewStatus == OrderStatus.Filled && request.FillPrice.HasValue)
        {
            order.MarkFilled(request.FillPrice.Value);
            await _orderRepository.UpdateAsync(order, cancellationToken).ConfigureAwait(false);
            await _UpdatePositionAsync(order, request.FillPrice.Value, cancellationToken)
                .ConfigureAwait(false);
        }
        else if (request.NewStatus == OrderStatus.Rejected)
        {
            order.MarkRejected();
            await _orderRepository.UpdateAsync(order, cancellationToken).ConfigureAwait(false);
        }

        LogStatusUpdated(_logger, order.Id, request.NewStatus);
        return Unit.Value;
    }

    private async Task _UpdatePositionAsync(
        Order order,
        decimal fillPrice,
        CancellationToken cancellationToken)
    {
        Position? position = await _positionRepository
            .GetAsync(order.TenantId, order.Symbol, cancellationToken)
            .ConfigureAwait(false);

        if (position is null)
        {
            position = Position.CreateFlat(order.TenantId, order.Symbol);
        }

        position.ApplyFill(order.Side, order.Quantity, fillPrice);
        await _positionRepository.SaveAsync(position, cancellationToken).ConfigureAwait(false);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Order {OrderId} not found for status update.")]
    private static partial void LogOrderNotFound(ILogger logger, Guid orderId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Order {OrderId} status updated to {Status}.")]
    private static partial void LogStatusUpdated(ILogger logger, Guid orderId, OrderStatus status);
}
