using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Prometheus;
using TradingPlatform.Common.Kafka;
using TradingPlatform.OMS.Application.Settings;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Interfaces;
using TradingPlatform.OMS.Infrastructure.Persistence;

namespace TradingPlatform.OMS.Infrastructure.Reconciliation;

/// <summary>
/// Runs once at OMS startup before the Kafka consumer begins processing.
/// Detects ghost trades — orders that EMS sent to Binance but crashed before
/// publishing the fill event — and repairs local order + position state.
///
/// Flow per ghost trade:
///   1. Found: Validated order older than <see cref="_minAge"/> in local DB.
///   2. Query: GET /api/v3/order?origClientOrderId={orderId} on Binance.
///   3a. Binance says FILLED → MarkFilled, ApplyFill position, publish fill event.
///   3b. Binance says not found / not filled → leave for StuckOrderReconciliationService.
/// </summary>
public sealed partial class ExchangeReconciliationService : IHostedService
{
    /// <summary>
    /// Only check Validated orders older than this — fresh ones may still have
    /// their fill event in-flight through Kafka.
    /// </summary>
    private static readonly TimeSpan _minAge = TimeSpan.FromSeconds(60);

    private static readonly Counter _ghostTradesRecovered = Metrics.CreateCounter(
        "oms_ghost_trades_recovered_total",
        "Orders repaired by exchange reconciliation on startup.");

    private static readonly Counter _reconciliationErrors = Metrics.CreateCounter(
        "oms_reconciliation_errors_total",
        "Errors encountered during exchange reconciliation.");

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<ExchangeReconciliationService> _logger;
    private readonly string _fillsTopic;

    /// <summary>Initializes the service.</summary>
    public ExchangeReconciliationService(
        IServiceScopeFactory scopeFactory,
        ILogger<ExchangeReconciliationService> logger,
        IOptions<OmsTopicSettings> topics)
    {
        ArgumentNullException.ThrowIfNull(topics);
        _scopeFactory = scopeFactory;
        _logger       = logger;
        _fillsTopic   = topics.Value.FillsTopic;
    }

    /// <summary>
    /// Runs full reconciliation synchronously so Kafka consumers only start
    /// after all ghost trades are repaired.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        LogStarted(_logger);

        using IServiceScope scope = _scopeFactory.CreateScope();

        IOrderRepository       orderRepo    = scope.ServiceProvider.GetRequiredService<IOrderRepository>();
        IPositionRepository    positionRepo = scope.ServiceProvider.GetRequiredService<IPositionRepository>();
        IBinanceAccountClient  binance      = scope.ServiceProvider.GetRequiredService<IBinanceAccountClient>();
        IKafkaProducer         kafka        = scope.ServiceProvider.GetRequiredService<IKafkaProducer>();
        TenantContext          tenantCtx    = scope.ServiceProvider.GetRequiredService<TenantContext>();

        IReadOnlyList<Order> candidates =
            await orderRepo.GetStuckAsync(_minAge, cancellationToken).ConfigureAwait(false);

        // Only Validated orders can be ghost trades — Pending means EMS never received them.
        IReadOnlyList<Order> validated = candidates
            .Where(o => o.Status == OrderStatus.Validated)
            .ToList()
            .AsReadOnly();

        if (validated.Count == 0)
        {
            LogNoCandidates(_logger);
            return;
        }

        LogCandidatesFound(_logger, validated.Count);

        foreach (Order order in validated)
        {
            await _ReconcileOrderAsync(
                order, orderRepo, positionRepo, binance, kafka, tenantCtx, cancellationToken)
                .ConfigureAwait(false);
        }

        LogCompleted(_logger);
    }

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task _ReconcileOrderAsync(
        Order order,
        IOrderRepository orderRepo,
        IPositionRepository positionRepo,
        IBinanceAccountClient binance,
        IKafkaProducer kafka,
        TenantContext tenantCtx,
        CancellationToken ct)
    {
        BinanceOrderQueryResult? binanceOrder;

        try
        {
            binanceOrder = await binance
                .GetOrderByClientIdAsync(order.Symbol, order.Id.ToString(), ct)
                .ConfigureAwait(false);
        }
        catch (HttpRequestException ex)
        {
            // Binance unreachable during reconciliation — skip this order,
            // StuckOrderReconciliationService will handle it later.
            LogBinanceUnreachable(_logger, order.Id, ex.Message);
            _reconciliationErrors.Inc();
            return;
        }

        if (binanceOrder is null || !binanceOrder.IsFilled)
        {
            // Not on Binance or not yet filled — not a ghost trade.
            LogNotAGhostTrade(_logger, order.Id, binanceOrder?.Status ?? "NOT_FOUND");
            return;
        }

        // ── Ghost trade confirmed — repair local state ──────────────────────────
        LogGhostTradeDetected(_logger, order.Id, order.Symbol, order.TenantId, binanceOrder.AvgFillPrice);

        tenantCtx.TenantId = order.TenantId;

        try
        {
            order.MarkFilled(binanceOrder.AvgFillPrice);
        }
        catch (InvalidOperationException ex)
        {
            // Order transitioned between scan and now — skip.
            LogSkipped(_logger, order.Id, ex.Message);
            return;
        }

        await orderRepo.UpdateAsync(order, ct).ConfigureAwait(false);

        // Update position — same logic as OrderFillsConsumerService.
        Position position =
            await positionRepo.GetAsync(order.TenantId, order.Symbol, ct).ConfigureAwait(false)
            ?? Position.CreateFlat(order.TenantId, order.Symbol);

        position.ApplyFill(order.Side, order.Quantity, binanceOrder.AvgFillPrice);
        await positionRepo.SaveAsync(position, ct).ConfigureAwait(false);

        // Publish fill event so any downstream consumers (e.g. Redis PnL pub) stay in sync.
        await kafka.PublishAsync(
            topic: _fillsTopic,
            key:   order.TenantId.ToString(),
            value: new OrderFillEvent
            {
                OrderId         = order.Id,
                TenantId        = order.TenantId,
                FillPrice       = binanceOrder.AvgFillPrice,
                ExchangeOrderId = 0,   // not available from origClientOrderId lookup
            },
            ct)
            .ConfigureAwait(false);

        _ghostTradesRecovered.Inc();

        LogGhostTradeRepaired(_logger, order.Id, order.TenantId, binanceOrder.AvgFillPrice);
    }

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Exchange reconciliation started — checking for ghost trades.")]
    private static partial void LogStarted(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "No Validated orders older than 60s — nothing to reconcile.")]
    private static partial void LogNoCandidates(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Found {Count} Validated order(s) to verify against Binance.")]
    private static partial void LogCandidatesFound(ILogger logger, int count);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Exchange reconciliation completed.")]
    private static partial void LogCompleted(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Binance unreachable while reconciling order {OrderId}: {Error}. " +
                  "StuckOrderReconciliationService will handle it.")]
    private static partial void LogBinanceUnreachable(ILogger logger, Guid orderId, string error);

    [LoggerMessage(Level = LogLevel.Debug,
        Message = "Order {OrderId} is not a ghost trade (Binance status: {BinanceStatus}).")]
    private static partial void LogNotAGhostTrade(ILogger logger, Guid orderId, string binanceStatus);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Ghost trade detected: order {OrderId} ({Symbol}) for tenant {TenantId} " +
                  "was filled on Binance at {FillPrice} but OMS has no fill record.")]
    private static partial void LogGhostTradeDetected(
        ILogger logger, Guid orderId, string symbol, Guid tenantId, decimal fillPrice);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Ghost trade repaired: order {OrderId} for tenant {TenantId} " +
                  "marked Filled at {FillPrice}, position updated.")]
    private static partial void LogGhostTradeRepaired(
        ILogger logger, Guid orderId, Guid tenantId, decimal fillPrice);

    [LoggerMessage(Level = LogLevel.Debug,
        Message = "Skipped order {OrderId} during reconciliation: {Reason}.")]
    private static partial void LogSkipped(ILogger logger, Guid orderId, string reason);
}

/// <summary>Fill event published to order-fills after a ghost trade is repaired.</summary>
internal sealed record OrderFillEvent
{
    public required Guid    OrderId         { get; init; }
    public required Guid    TenantId        { get; init; }
    public required decimal FillPrice       { get; init; }
    public required long    ExchangeOrderId { get; init; }
}
