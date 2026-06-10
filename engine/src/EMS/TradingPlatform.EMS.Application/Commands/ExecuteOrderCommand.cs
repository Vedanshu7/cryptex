using MediatR;

namespace TradingPlatform.EMS.Application.Commands;

/// <summary>
/// Command to execute a validated order on the exchange.
/// Deserialised from the validated-orders Kafka topic.
/// </summary>
public sealed record ExecuteOrderCommand : IRequest<ExecuteOrderResult>
{
    /// <summary>Gets the OMS order ID to execute.</summary>
    public required Guid OrderId { get; init; }

    /// <summary>Gets the tenant that placed the order.</summary>
    public required Guid TenantId { get; init; }

    /// <summary>Gets the trading pair symbol.</summary>
    public required string Symbol { get; init; }

    /// <summary>Gets the order direction (BUY or SELL).</summary>
    public required string Side { get; init; }

    /// <summary>Gets the requested quantity.</summary>
    public required decimal Quantity { get; init; }
}

/// <summary>Result returned by <see cref="ExecuteOrderCommand"/>.</summary>
public sealed record ExecuteOrderResult
{
    /// <summary>Gets whether the execution was successful.</summary>
    public required bool Success { get; init; }

    /// <summary>Gets the Binance order ID when successful.</summary>
    public long? ExchangeOrderId { get; init; }

    /// <summary>Gets the actual fill price when successful.</summary>
    public decimal? FillPrice { get; init; }

    /// <summary>Gets the error message when <see cref="Success"/> is false.</summary>
    public string? ErrorMessage { get; init; }
}
