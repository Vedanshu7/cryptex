using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Infrastructure.Persistence.Configurations;

/// <summary>EF Core configuration for the Order entity.</summary>
#pragma warning disable CA1812 // Instantiated by EF Core via ApplyConfigurationsFromAssembly.
internal sealed class OrderConfiguration : IEntityTypeConfiguration<Order>
{
    /// <summary>
    /// Maps <see cref="OrderStatus"/> to the exact SCREAMING_SNAKE_CASE labels
    /// of the Postgres `order_status` enum type (migration 002) — a plain
    /// <c>HasConversion&lt;string&gt;()</c> would send the C# member's
    /// PascalCase <c>ToString()</c> (e.g. "Pending"), which Postgres's native
    /// enum type rejects outright (only "PENDING" etc. are valid labels).
    /// Dictionary indexers (not switch/throw expressions, which EF Core's
    /// LINQ-to-SQL translator can't turn into an expression tree) back both
    /// directions of the conversion.
    /// </summary>
    private static readonly IReadOnlyDictionary<OrderStatus, string> _ToDb = new Dictionary<OrderStatus, string>
    {
        [OrderStatus.Pending]       = "PENDING",
        [OrderStatus.Validated]     = "VALIDATED",
        [OrderStatus.Filled]        = "FILLED",
        [OrderStatus.PartialFilled] = "PARTIAL_FILLED",
        [OrderStatus.Rejected]      = "REJECTED",
        [OrderStatus.Cancelled]     = "CANCELLED",
    };

    private static readonly IReadOnlyDictionary<string, OrderStatus> _FromDb = new Dictionary<string, OrderStatus>
    {
        ["PENDING"]        = OrderStatus.Pending,
        ["VALIDATED"]      = OrderStatus.Validated,
        ["FILLED"]         = OrderStatus.Filled,
        ["PARTIAL_FILLED"] = OrderStatus.PartialFilled,
        ["REJECTED"]       = OrderStatus.Rejected,
        ["CANCELLED"]      = OrderStatus.Cancelled,
    };

    private static readonly ValueConverter<OrderStatus, string> _StatusConverter =
        new(toDb => _ToDb[toDb], fromDb => _FromDb[fromDb]);

    /// <inheritdoc/>
    public void Configure(EntityTypeBuilder<Order> builder)
    {
        builder.ToTable("orders");
        builder.HasKey(o => o.Id);

        builder.Property(o => o.Id).HasColumnName("id");
        builder.Property(o => o.TenantId).HasColumnName("tenant_id").IsRequired();
        builder.Property(o => o.Symbol).HasColumnName("symbol").HasMaxLength(20).IsRequired();
        builder.Property(o => o.Side).HasColumnName("side").HasMaxLength(4).IsRequired();
        builder.Property(o => o.Quantity).HasColumnName("quantity").HasPrecision(18, 8).IsRequired();
        builder.Property(o => o.Price).HasColumnName("price").HasPrecision(18, 8);
        builder.Property(o => o.Status)
            .HasColumnName("status")
            .HasConversion(_StatusConverter)
            .IsRequired();
        builder.Property(o => o.SignalId).HasColumnName("signal_id").HasMaxLength(100);
        builder.Property(o => o.CreatedAt).HasColumnName("created_at").IsRequired();
        builder.Property(o => o.FilledAt).HasColumnName("filled_at");

        builder.HasIndex(o => new { o.TenantId, o.SignalId })
               .IsUnique()
               .HasFilter("signal_id IS NOT NULL")
               .HasDatabaseName("uq_orders_tenant_signal");
    }
}
