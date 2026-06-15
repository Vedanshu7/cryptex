using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Prometheus;
using TradingPlatform.OMS.Domain.Entities;
using TradingPlatform.OMS.Domain.Interfaces;
using TradingPlatform.OMS.Infrastructure.Persistence;

namespace TradingPlatform.OMS.Infrastructure.Kafka;

/// <summary>
/// Periodic background service that finds orders stuck in Pending or Validated
/// status and cancels them.
///
/// Why orders get stuck: if the EMS crashes after hitting Binance but before
/// publishing to the order-fills topic, the OMS never receives the fill event
/// and the order stays Validated indefinitely.
///
/// Limitation: because the OMS does not store the Binance exchange order ID,
/// this service cannot verify the fill on Binance's side — it conservatively
/// cancels the order and emits a warning for manual review. A future improvement
/// would thread newClientOrderId through the EMS so fills can be confirmed via
/// GET /api/v3/order before cancellation.
/// </summary>
public sealed partial class StuckOrderReconciliationService : BackgroundService
{
    private static readonly TimeSpan _scanInterval  = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan _stuckThreshold = TimeSpan.FromMinutes(5);

    private static readonly Counter _reconciliationsTotal = Metrics.CreateCounter(
        "oms_stuck_orders_cancelled_total",
        "Total orders cancelled by the reconciliation service.",
        new CounterConfiguration { LabelNames = ["prior_status"] });

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<StuckOrderReconciliationService> _logger;

    /// <summary>Initializes the service.</summary>
    public StuckOrderReconciliationService(
        IServiceScopeFactory scopeFactory,
        ILogger<StuckOrderReconciliationService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger       = logger;
    }

    /// <inheritdoc/>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogStarted(_logger);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(_scanInterval, stoppingToken).ConfigureAwait(false);

            try
            {
                await _ReconcileAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Microsoft.EntityFrameworkCore.DbUpdateException ex)
            {
                LogScanError(_logger, ex);
                // DB error — continue the loop so a transient failure doesn't stop the service.
            }
        }

        LogStopped(_logger);
    }

    // Internal so unit tests can invoke a single reconciliation pass directly
    // without running the full BackgroundService loop.
    internal Task ReconcileForTestAsync(CancellationToken ct) => _ReconcileAsync(ct);

    private async Task _ReconcileAsync(CancellationToken ct)
    {
        using IServiceScope scope = _scopeFactory.CreateScope();

        IOrderRepository orderRepo =
            scope.ServiceProvider.GetRequiredService<IOrderRepository>();

        IReadOnlyList<Order> stuckOrders =
            await orderRepo.GetStuckAsync(_stuckThreshold, ct).ConfigureAwait(false);

        if (stuckOrders.Count == 0)
        {
            return;
        }

        LogStuckFound(_logger, stuckOrders.Count, (int)_stuckThreshold.TotalMinutes);

        // The TenantDbCommandInterceptor reads from the scoped TenantContext.
        // Set it per-order so UPDATE commands include the correct SET LOCAL
        // app.current_tenant_id and RLS allows the write.
        TenantContext tenantContext =
            scope.ServiceProvider.GetRequiredService<TenantContext>();

        foreach (Order order in stuckOrders)
        {
            OrderStatus priorStatus = order.Status;

            try
            {
                order.MarkCancelled();
            }
            catch (InvalidOperationException ex)
            {
                // Order transitioned to a terminal state between the scan and now — skip.
                LogSkipped(_logger, order.Id, ex.Message);
                continue;
            }

            tenantContext.TenantId = order.TenantId;
            await orderRepo.UpdateAsync(order, ct).ConfigureAwait(false);

            _reconciliationsTotal.WithLabels(priorStatus.ToString()).Inc();
            LogCancelled(_logger, order.Id, priorStatus, order.TenantId);
        }
    }

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Stuck-order reconciliation service started (scan every {ScanSeconds}s, threshold {ThresholdMinutes}m).")]
    private static partial void LogStarted(ILogger logger,
        int scanSeconds = 30, int thresholdMinutes = 5);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Stuck-order reconciliation service stopped.")]
    private static partial void LogStopped(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Found {Count} stuck order(s) older than {ThresholdMinutes} minute(s). Cancelling.")]
    private static partial void LogStuckFound(ILogger logger, int count, int thresholdMinutes);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Reconciliation cancelled order {OrderId} (was {PriorStatus}) for tenant {TenantId}. " +
                  "If the EMS had already placed this on Binance, a manual fill check is required.")]
    private static partial void LogCancelled(ILogger logger, Guid orderId, OrderStatus priorStatus, Guid tenantId);

    [LoggerMessage(Level = LogLevel.Debug,
        Message = "Skipped order {OrderId} during reconciliation: {Reason}")]
    private static partial void LogSkipped(ILogger logger, Guid orderId, string reason);

    [LoggerMessage(Level = LogLevel.Error,
        Message = "Unhandled error during stuck-order reconciliation scan.")]
    private static partial void LogScanError(ILogger logger, Exception ex);
}
