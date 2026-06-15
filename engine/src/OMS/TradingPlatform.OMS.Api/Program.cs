using MediatR;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;
using Serilog;
using Serilog.Formatting.Json;
using TradingPlatform.Common.Kafka;
using TradingPlatform.OMS.Api.Middleware;
using TradingPlatform.OMS.Application.Handlers;
using TradingPlatform.OMS.Application.Settings;
using TradingPlatform.OMS.Domain.Interfaces;
using TradingPlatform.OMS.Infrastructure.Exchange;
using TradingPlatform.OMS.Infrastructure.Kafka;
using TradingPlatform.OMS.Infrastructure.Persistence;
using TradingPlatform.OMS.Infrastructure.Reconciliation;
using TradingPlatform.OMS.Infrastructure.Repositories;
using TradingPlatform.OMS.Infrastructure.Services;

// ── Serilog ───────────────────────────────────────────────────────────────────
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console(new JsonFormatter())
    .Enrich.FromLogContext()
    .CreateLogger();

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Host.UseSerilog();

// ── Tenant context (RLS) ─────────────────────────────────────────────────────
// Scoped so each request gets its own TenantContext populated by middleware.
builder.Services.AddScoped<TenantContext>();
builder.Services.AddScoped<ITenantContext>(sp => sp.GetRequiredService<TenantContext>());

// ── Database ──────────────────────────────────────────────────────────────────
// DbContext receives ITenantContext for both global query filters (EF layer)
// and the command interceptor (SQL layer) — two lines of tenant isolation defence.
builder.Services.AddDbContext<TradingDbContext>((sp, options) =>
{
    options.UseNpgsql(builder.Configuration.GetConnectionString("Postgres"));
    ITenantContext tenantCtx = sp.GetRequiredService<ITenantContext>();
    options.AddInterceptors(new TenantDbCommandInterceptor(tenantCtx));
});

// ── Domain services ───────────────────────────────────────────────────────────
builder.Services.AddScoped<IOrderRepository, OrderRepository>();
builder.Services.AddScoped<IPositionRepository, PositionRepository>();
builder.Services.AddScoped<IRiskService, BasicRiskService>();

// ── Binance account client (reconciliation only) ──────────────────────────────
builder.Services.Configure<BinanceAccountSettings>(
    builder.Configuration.GetSection("Binance"));
builder.Services.AddHttpClient<IBinanceAccountClient, BinanceAccountClient>(
    (sp, http) =>
    {
        BinanceAccountSettings settings = sp
            .GetRequiredService<Microsoft.Extensions.Options.IOptions<BinanceAccountSettings>>()
            .Value;
        http.BaseAddress = settings.BaseUrl;
    });

// ── MediatR ───────────────────────────────────────────────────────────────────
builder.Services.AddMediatR(cfg =>
    cfg.RegisterServicesFromAssembly(typeof(PlaceOrderHandler).Assembly));

// ── Topic config (override per region via OMS__Topics__* env vars) ───────────
builder.Services.Configure<OmsTopicSettings>(
    builder.Configuration.GetSection("OMS:Topics"));

// ── Kafka ─────────────────────────────────────────────────────────────────────
builder.Services.Configure<KafkaSettings>(
    builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();
// ExchangeReconciliationService runs first (IHostedService.StartAsync blocks until complete)
// so ghost trades are repaired before Kafka consumers begin processing new messages.
builder.Services.AddHostedService<ExchangeReconciliationService>();    // repairs ghost trades on startup
builder.Services.AddHostedService<KafkaConsumerService>();             // reads order-requests
builder.Services.AddHostedService<OrderFillsConsumerService>();        // reads order-fills → closes the loop
builder.Services.AddHostedService<StuckOrderReconciliationService>();  // cancels stuck Pending/Validated orders

// ── OpenTelemetry ─────────────────────────────────────────────────────────────
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("oms"))
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddEntityFrameworkCoreInstrumentation()
        .AddOtlpExporter(o =>
            o.Endpoint = new Uri(
                builder.Configuration["Otel:Endpoint"] ?? "http://jaeger:4317")));

// ── Controllers ───────────────────────────────────────────────────────────────
builder.Services.AddControllers();

WebApplication app = builder.Build();

app.UseMiddleware<CorrelationIdMiddleware>();
app.UseMiddleware<TenantResolutionMiddleware>();
app.UseHttpMetrics();       // Prometheus per-route metrics.
app.MapMetrics();           // GET /metrics endpoint.
app.MapControllers();

app.Run();
