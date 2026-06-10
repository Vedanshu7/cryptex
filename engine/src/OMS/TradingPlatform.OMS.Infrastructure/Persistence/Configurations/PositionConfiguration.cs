using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.Infrastructure.Persistence.Configurations;

/// <summary>EF Core configuration for the Position entity.</summary>
#pragma warning disable CA1812
internal sealed class PositionConfiguration : IEntityTypeConfiguration<Position>
{
    /// <inheritdoc/>
    public void Configure(EntityTypeBuilder<Position> builder)
    {
        builder.ToTable("positions");
        builder.HasKey(p => p.Id);

        builder.Property(p => p.Id).HasColumnName("id");
        builder.Property(p => p.TenantId).HasColumnName("tenant_id").IsRequired();
        builder.Property(p => p.Symbol).HasColumnName("symbol").HasMaxLength(20).IsRequired();
        builder.Property(p => p.Quantity).HasColumnName("quantity").HasPrecision(18, 8).IsRequired();
        builder.Property(p => p.AvgPrice).HasColumnName("avg_price").HasPrecision(18, 8).IsRequired();
        builder.Property(p => p.UpdatedAt).HasColumnName("updated_at").IsRequired();

        builder.HasIndex(p => new { p.TenantId, p.Symbol }).IsUnique();
    }
}
