using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace TradingPlatform.OMS.Infrastructure.Persistence;

/// <summary>
/// EF Core command interceptor that prepends
/// <c>SET LOCAL app.current_tenant_id = '...'</c>
/// to every command when a tenant context is active.
///
/// This is what makes PostgreSQL RLS policies enforce row-level isolation —
/// the policy reads <c>current_setting('app.current_tenant_id', TRUE)</c>.
/// Without this interceptor, RLS returns zero rows for all tenant-scoped tables.
/// </summary>
public sealed class TenantDbCommandInterceptor : DbCommandInterceptor
{
    private readonly ITenantContext _tenantContext;

    /// <summary>Initializes the interceptor with the current request's tenant context.</summary>
    public TenantDbCommandInterceptor(ITenantContext tenantContext)
    {
        _tenantContext = tenantContext;
    }

    /// <inheritdoc/>
    public override InterceptionResult<DbDataReader> ReaderExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<DbDataReader> result)
    {
        ArgumentNullException.ThrowIfNull(command);
        _InjectTenantId(command);
        return result;
    }

    /// <inheritdoc/>
    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<DbDataReader> result,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        _InjectTenantId(command);
        return ValueTask.FromResult(result);
    }

    /// <inheritdoc/>
    public override InterceptionResult<int> NonQueryExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<int> result)
    {
        ArgumentNullException.ThrowIfNull(command);
        _InjectTenantId(command);
        return result;
    }

    /// <inheritdoc/>
    public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        _InjectTenantId(command);
        return ValueTask.FromResult(result);
    }

    private void _InjectTenantId(DbCommand command)
    {
        if (_tenantContext.TenantId is not { } tenantId)
        {
            return;
        }

        // tenantId is a Guid struct (not user input) — its ToString() produces only
        // hex digits and hyphens, so there is no SQL injection surface here.
#pragma warning disable CA2100
        command.CommandText =
            $"SET LOCAL app.current_tenant_id = '{tenantId}';\n"
            + command.CommandText;
#pragma warning restore CA2100
    }
}
