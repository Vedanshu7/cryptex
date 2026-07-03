using Confluent.Kafka;
using MediatR;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;
using Serilog;
using Serilog.Formatting.Json;
using TradingPlatform.Common.Kafka;
using TradingPlatform.Common.Universe;
using TradingPlatform.EMS.Application.Handlers;
using TradingPlatform.EMS.Application.Settings;
using TradingPlatform.EMS.Domain.Interfaces;
using TradingPlatform.EMS.Infrastructure.Exchange;
using TradingPlatform.EMS.Infrastructure.Kafka;
using TradingPlatform.EMS.Infrastructure.Persistence;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console(new JsonFormatter())
    .Enrich.FromLogContext()
    .CreateLogger();

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Host.UseSerilog();

// ── Binance client ────────────────────────────────────────────────────────────
builder.Services.Configure<BinanceSettings>(
    builder.Configuration.GetSection("Binance"));
builder.Services.AddHttpClient<IBinanceClient, BinanceClient>(
    (sp, http) =>
    {
        BinanceSettings settings = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<BinanceSettings>>().Value;
        http.BaseAddress = settings.BaseUrl;
    });

// ── Execution persistence ─────────────────────────────────────────────────────
builder.Services.Configure<ExecutionDbSettings>(
    builder.Configuration.GetSection("ExecutionDb"));
builder.Services.AddScoped<IExecutionRepository, ExecutionRepository>();

// ── MediatR ───────────────────────────────────────────────────────────────────
builder.Services.AddMediatR(cfg =>
    cfg.RegisterServicesFromAssembly(typeof(ExecuteOrderHandler).Assembly));

// ── Topic config (override per region via EMS__Topics__* env vars) ───────────
builder.Services.Configure<EmsTopicSettings>(
    builder.Configuration.GetSection("EMS:Topics"));

// Deriving topic names from Region (rather than requiring both spelled out)
// removes a class of typo where OMS and EMS's hand-written topic names for
// the same region silently drift apart. Also fails fast at startup if Region
// isn't a real entry in universe.json. See EmsTopicSettingsDeriver for the
// (unit-tested) derivation/validation logic itself.
builder.Services.PostConfigure<EmsTopicSettings>(settings =>
{
    if (string.IsNullOrEmpty(settings.Region))
    {
        return;
    }

    string universeFilePath = builder.Configuration["UNIVERSE_FILE"] ?? "universe.json";
    TradingUniverse universe = TradingUniverse.Load(universeFilePath);

    EmsTopicSettingsDeriver.DeriveFromRegion(settings, universe.Regions.Keys.ToList());
});

// ── Kafka ─────────────────────────────────────────────────────────────────────
builder.Services.Configure<KafkaSettings>(
    builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();

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
    EmsTopicSettings topics = sp
        .GetRequiredService<Microsoft.Extensions.Options.IOptions<EmsTopicSettings>>().Value;
    return new List<TopicSpec>
    {
        new() { Name = topics.InputTopic },
        new() { Name = topics.OutputTopic },
    };
});

// Must run first — IHostedService.StartAsync calls run in registration order,
// so topics are guaranteed to exist before the consumer below subscribes.
builder.Services.AddHostedService<KafkaTopicProvisioningService>();
builder.Services.AddHostedService<EmsKafkaConsumerService>();

// ── OpenTelemetry ─────────────────────────────────────────────────────────────
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("ems"))
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter(o =>
            o.Endpoint = new Uri(
                builder.Configuration["Otel:Endpoint"] ?? "http://jaeger:4317")));

builder.Services.AddControllers();

WebApplication app = builder.Build();
app.UseHttpMetrics();
app.MapMetrics();
app.MapControllers();
app.Run();
