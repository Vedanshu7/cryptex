using Confluent.Kafka;
using MediatR;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;
using Serilog;
using Serilog.Formatting.Json;
using TradingPlatform.Common.Kafka;
using TradingPlatform.Common.Universe;
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

// Deriving topic names from Region (rather than requiring all three spelled
// out) removes a class of typo where OMS and EMS's hand-written topic names
// for the same region silently drift apart. Also fails fast at startup if
// Region isn't a real entry in universe.json — better than an OMS quietly
// consuming from a topic no signal will ever reach. See OmsTopicSettingsDeriver
// for the (unit-tested) derivation/validation logic itself.
builder.Services.PostConfigure<OmsTopicSettings>(settings =>
{
    if (string.IsNullOrEmpty(settings.Region))
    {
        return;
    }

    string universeFilePath = builder.Configuration["UNIVERSE_FILE"] ?? "universe.json";
    TradingUniverse universe = TradingUniverse.Load(universeFilePath);

    OmsTopicSettingsDeriver.DeriveFromRegion(settings, universe.Regions.Keys.ToList());
});

// ── Kafka ─────────────────────────────────────────────────────────────────────
builder.Services.Configure<KafkaSettings>(
    builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();
builder.Services.AddSingleton<IDeadLetterPublisher, DeadLetterPublisher>();
builder.Services.AddSingleton<IRetryingDlqDispatcher>(sp => new RetryingDlqDispatcher(
    sp.GetRequiredService<IDeadLetterPublisher>(),
    sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<RetryingDlqDispatcher>>(),
    serviceName: "oms"));

// ── Kafka topic provisioning ─────────────────────────────────────────────────
// Lets a new region come up from config alone — no manual create-topics.sh run.
// Topic-already-exists is treated as success, so this is safe on every restart.
builder.Services.AddSingleton<IAdminClient>(sp =>
{
    KafkaSettings settings = sp
        .GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaSettings>>().Value;
    return new AdminClientBuilder(new AdminClientConfig { BootstrapServers = settings.Brokers }).Build();
});
builder.Services.AddSingleton<IKafkaTopicProvisioner, KafkaTopicProvisioner>();
builder.Services.AddSingleton<IReadOnlyList<TopicSpec>>(sp =>
{
    OmsTopicSettings topics = sp
        .GetRequiredService<Microsoft.Extensions.Options.IOptions<OmsTopicSettings>>().Value;
    return new List<TopicSpec>
    {
        new() { Name = topics.InputTopic },
        new() { Name = topics.OutputTopic },
        new() { Name = topics.FillsTopic },
        new() { Name = $"{topics.InputTopic}.dlq" },
        new() { Name = $"{topics.FillsTopic}.dlq" },
    };
});

// IHostedService.StartAsync calls run in registration order, each awaited before
// the next starts — this is what guarantees topics exist before anything below
// tries to consume or produce, and ghost trades are repaired before new messages flow.
builder.Services.AddHostedService<KafkaTopicProvisioningService>();    // creates topics if missing
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
