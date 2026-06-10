using System.Data;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using TradingPlatform.EMS.Domain.Entities;
using TradingPlatform.EMS.Domain.Interfaces;

namespace TradingPlatform.EMS.Infrastructure.Persistence;

/// <summary>
/// Persists execution records to PostgreSQL using raw Npgsql (no EF Core).
/// EMS is write-only for execution records — no complex query model needed.
/// </summary>
public sealed partial class ExecutionRepository : IExecutionRepository
{
    private readonly string _connectionString;
    private readonly ILogger<ExecutionRepository> _logger;

    /// <summary>Initializes the repository with the database connection string.</summary>
    public ExecutionRepository(
        IOptions<ExecutionDbSettings> settings,
        ILogger<ExecutionRepository> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        _connectionString = settings.Value.ConnectionString;
        _logger           = logger;
    }

    /// <inheritdoc/>
    public async Task SaveAsync(Execution execution, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(execution);

        NpgsqlConnection conn = new(_connectionString);
        await using (conn.ConfigureAwait(false))
        {
        await conn.OpenAsync(ct).ConfigureAwait(false);

        const string sql = """
            INSERT INTO executions
                (id, order_id, tenant_id, exchange_order_id, fill_price, status, error_message, executed_at)
            VALUES
                (@id, @orderId, @tenantId, @exchangeOrderId, @fillPrice, @status, @errorMessage, @executedAt)
            """;

        NpgsqlCommand cmd = new(sql, conn);
        await using (cmd.ConfigureAwait(false))
        {
        cmd.Parameters.AddWithValue("id",              execution.Id);
        cmd.Parameters.AddWithValue("orderId",         execution.OrderId);
        cmd.Parameters.AddWithValue("tenantId",        execution.TenantId);
        cmd.Parameters.AddWithValue("exchangeOrderId", execution.ExchangeOrderId.HasValue
            ? (object)execution.ExchangeOrderId.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("fillPrice",       execution.FillPrice);
        cmd.Parameters.AddWithValue("status",          execution.Status.ToString());
        cmd.Parameters.AddWithValue("errorMessage",    execution.ErrorMessage is not null
            ? (object)execution.ErrorMessage : DBNull.Value);
        cmd.Parameters.AddWithValue("executedAt",      execution.ExecutedAt);

        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        } // end cmd
        } // end conn
        LogSaved(_logger, execution.Id, execution.OrderId);
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Execution {ExecutionId} saved for order {OrderId}.")]
    private static partial void LogSaved(ILogger logger, Guid executionId, Guid orderId);
}

/// <summary>Database connection settings for the execution repository.</summary>
public sealed record ExecutionDbSettings
{
    /// <summary>Gets the Npgsql connection string.</summary>
    public required string ConnectionString { get; init; }
}
