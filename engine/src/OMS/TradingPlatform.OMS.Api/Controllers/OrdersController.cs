using MediatR;
using Microsoft.AspNetCore.Mvc;
using TradingPlatform.OMS.Application.Commands;
using TradingPlatform.OMS.Application.Queries;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Api.Controllers;

/// <summary>REST API surface for order management operations.</summary>
[ApiController]
[Route("api/orders")]
public sealed class OrdersController : ControllerBase
{
    private readonly IMediator _mediator;

    /// <summary>Initializes the controller.</summary>
    public OrdersController(IMediator mediator)
    {
        _mediator = mediator;
    }

    /// <summary>Retrieves a single order by ID.</summary>
    [HttpGet("{id:guid}")]
    public async Task<IActionResult> GetByIdAsync(
        Guid id,
        [FromHeader(Name = "X-Tenant-ID")] Guid tenantId,
        CancellationToken ct)
    {
        Order? order = await _mediator
            .Send(new GetOrderByIdQuery { OrderId = id, TenantId = tenantId }, ct)
            .ConfigureAwait(false);

        return order is null ? NotFound() : Ok(order);
    }

    /// <summary>Places a new order via REST (bypasses Kafka — useful for testing).</summary>
    [HttpPost]
    public async Task<IActionResult> PlaceAsync(
        [FromBody] PlaceOrderCommand command,
        CancellationToken ct)
    {
        PlaceOrderResult result = await _mediator
            .Send(command, ct)
            .ConfigureAwait(false);

        return result.Status == OrderStatus.Rejected
            ? BadRequest(result)
            : CreatedAtAction(
                nameof(GetByIdAsync),
                new { id = result.OrderId },
                result);
    }
}
