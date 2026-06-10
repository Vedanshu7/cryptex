using TradingPlatform.OMS.Infrastructure.Persistence;

namespace TradingPlatform.OMS.Api.Middleware;

/// <summary>
/// Resolves the current tenant from the X-Tenant-ID request header and
/// populates <see cref="TenantContext"/> so the EF Core interceptor
/// can inject it as a PostgreSQL session variable for RLS.
///
/// In production this would validate the tenant against a JWT claim or
/// an API key lookup — the header-only approach is intentional for local dev.
/// </summary>
public sealed class TenantResolutionMiddleware
{
    private const string TenantHeader = "X-Tenant-ID";
    private readonly RequestDelegate _next;

    /// <summary>Initializes the middleware.</summary>
    public TenantResolutionMiddleware(RequestDelegate next)
    {
        _next = next;
    }

    /// <summary>Executes the middleware.</summary>
    public async Task InvokeAsync(HttpContext context, TenantContext tenantContext)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(tenantContext);

        if (context.Request.Headers.TryGetValue(
                TenantHeader,
                out Microsoft.Extensions.Primitives.StringValues rawValue)
            && Guid.TryParse(rawValue.ToString(), out Guid tenantId))
        {
            tenantContext.TenantId = tenantId;
        }

        await _next(context).ConfigureAwait(false);
    }
}
