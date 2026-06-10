using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Infrastructure.Persistence.Configurations;

/// <summary>EF Core configuration for the Order entity.</summary>
#pragma warning disable CA1812 // Instantiated by EF Core via ApplyConfigurationsFromAssembly.
internal sealed class OrderConfiguration : IEntityTypeConfiguration<Order>
{
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
            .HasConversion<string>()
            .IsRequired();
        builder.Property(o => o.SignalId).HasColumnName("signal_id").HasMaxLength(100);
        builder.Property(o => o.CreatedAt).HasColumnName("created_at").IsRequired();
        builder.Property(o => o.FilledAt).HasColumnName("filled_at");
    }
}
