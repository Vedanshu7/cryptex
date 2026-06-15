using FluentAssertions;
using TradingPlatform.OMS.Domain.Entities;

namespace TradingPlatform.OMS.UnitTests.Domain;

public sealed class PositionTests
{
    [Fact]
    public void CreateFlat_InitialisesZeroFields()
    {
        Position pos = Position.CreateFlat(Guid.NewGuid(), "BTCUSDT");

        pos.Quantity.Should().Be(0m);
        pos.AvgPrice.Should().Be(0m);
        pos.RealisedPnl.Should().Be(0m);
    }

    [Fact]
    public void ApplyFill_Buy_UpdatesQuantityAndAvgPrice()
    {
        Position pos = Position.CreateFlat(Guid.NewGuid(), "BTCUSDT");

        pos.ApplyFill("BUY", 1m, 50_000m);

        pos.Quantity.Should().Be(1m);
        pos.AvgPrice.Should().Be(50_000m);
        pos.RealisedPnl.Should().Be(0m);
    }

    [Fact]
    public void ApplyFill_MultipleBuys_VwapAvgPrice()
    {
        Position pos = Position.CreateFlat(Guid.NewGuid(), "BTCUSDT");

        pos.ApplyFill("BUY", 1m, 40_000m);
        pos.ApplyFill("BUY", 1m, 60_000m);

        pos.Quantity.Should().Be(2m);
        pos.AvgPrice.Should().Be(50_000m);  // (40k + 60k) / 2
        pos.RealisedPnl.Should().Be(0m);
    }

    [Fact]
    public void ApplyFill_Sell_AccumulatesRealisedPnl()
    {
        Position pos = Position.CreateFlat(Guid.NewGuid(), "BTCUSDT");
        pos.ApplyFill("BUY", 2m, 50_000m);

        pos.ApplyFill("SELL", 1m, 55_000m);

        pos.Quantity.Should().Be(1m);
        pos.AvgPrice.Should().Be(50_000m);    // cost basis unchanged
        pos.RealisedPnl.Should().Be(5_000m);  // (55k - 50k) × 1
    }

    [Fact]
    public void ApplyFill_Sell_NegativePnlOnLoss()
    {
        Position pos = Position.CreateFlat(Guid.NewGuid(), "BTCUSDT");
        pos.ApplyFill("BUY", 1m, 50_000m);

        pos.ApplyFill("SELL", 1m, 45_000m);

        pos.RealisedPnl.Should().Be(-5_000m);
        pos.Quantity.Should().Be(0m);
    }

    [Fact]
    public void ApplyFill_MultipleSells_PnlAccumulates()
    {
        Position pos = Position.CreateFlat(Guid.NewGuid(), "BTCUSDT");
        pos.ApplyFill("BUY", 3m, 50_000m);

        pos.ApplyFill("SELL", 1m, 55_000m);  // +5k
        pos.ApplyFill("SELL", 1m, 48_000m);  // -2k

        pos.RealisedPnl.Should().Be(3_000m);
        pos.Quantity.Should().Be(1m);
    }

    [Fact]
    public void ApplyFill_SellExceedsHolding_QuantityClampedToZero()
    {
        Position pos = Position.CreateFlat(Guid.NewGuid(), "BTCUSDT");
        pos.ApplyFill("BUY", 1m, 50_000m);

        // Sell more than held — clamped to actual holding for P&L.
        pos.ApplyFill("SELL", 2m, 55_000m);

        pos.Quantity.Should().Be(0m);
        pos.RealisedPnl.Should().Be(5_000m);  // only 1 unit was actually closed
    }

    [Fact]
    public void ApplyFill_SellCaseInsensitive()
    {
        Position pos = Position.CreateFlat(Guid.NewGuid(), "BTCUSDT");
        pos.ApplyFill("BUY", 1m, 50_000m);

        pos.ApplyFill("sell", 1m, 60_000m);

        pos.RealisedPnl.Should().Be(10_000m);
    }
}
