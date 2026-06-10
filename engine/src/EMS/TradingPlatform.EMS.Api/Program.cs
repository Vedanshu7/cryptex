using MediatR;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;
using Serilog;
using Serilog.Formatting.Json;
using TradingPlatform.Common.Kafka;
using TradingPlatform.EMS.Application.Handlers;
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

// ── Kafka ─────────────────────────────────────────────────────────────────────
builder.Services.Configure<KafkaSettings>(
    builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();
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
