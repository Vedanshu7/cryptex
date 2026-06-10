using MediatR;
using TradingPlatform.OMS.Application.Queries;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Interfaces;

namespace TradingPlatform.OMS.Application.Handlers;

/// <summary>Handles <see cref="GetOrderByIdQuery"/>.</summary>
public sealed class GetOrderByIdHandler : IRequestHandler<GetOrderByIdQuery, Order?>
{
    private readonly IOrderRepository _orderRepository;

    /// <summary>Initializes the handler.</summary>
    public GetOrderByIdHandler(IOrderRepository orderRepository)
    {
        _orderRepository = orderRepository;
    }

    /// <inheritdoc/>
    public async Task<Order?> Handle(
        GetOrderByIdQuery request,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        return await _orderRepository
            .GetByIdAsync(request.OrderId, cancellationToken)
            .ConfigureAwait(false);
    }
}
