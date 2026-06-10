using Serilog.Context;

namespace TradingPlatform.OMS.Api.Middleware;

/// <summary>
/// Reads X-Correlation-ID from incoming requests (or generates one) and
/// pushes it into Serilog's LogContext so every log line includes it.
/// Also echoes the header back in the response.
/// </summary>
public sealed class CorrelationIdMiddleware
{
    private const string HeaderName = "X-Correlation-ID";
    private readonly RequestDelegate _next;

    /// <summary>Initializes the middleware.</summary>
    public CorrelationIdMiddleware(RequestDelegate next)
    {
        _next = next;
    }

    /// <summary>Executes the middleware.</summary>
    public async Task InvokeAsync(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        string correlationId = context.Request.Headers
            .TryGetValue(HeaderName, out Microsoft.Extensions.Primitives.StringValues existing)
                ? existing.ToString()
                : Guid.NewGuid().ToString();

        context.Response.Headers[HeaderName] = correlationId;

        using IDisposable _ = LogContext.PushProperty("CorrelationId", correlationId);
        await _next(context).ConfigureAwait(false);
    }
}
