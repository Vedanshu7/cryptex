using FluentAssertions;
using TradingPlatform.Common.Universe;

namespace TradingPlatform.OMS.UnitTests.Universe;

public sealed class TradingUniverseTests
{
    private static string WriteTempUniverse(string json)
    {
        string path = Path.Combine(Path.GetTempPath(), $"universe-{Guid.NewGuid()}.json");
        File.WriteAllText(path, json);
        return path;
    }

    [Fact]
    public void Load_ValidFile_ParsesRegionsAndSymbols()
    {
        string path = WriteTempUniverse(
            """
            {
              "version": 1,
              "regions": {
                "tokyo": {"exchange": "binance", "symbols": ["BTCUSDT", "ETHUSDT"]},
                "eu": {"exchange": "deribit", "symbols": ["BTC-PERPETUAL"]}
              }
            }
            """);

        try
        {
            TradingUniverse universe = TradingUniverse.Load(path);

            universe.Version.Should().Be(1);
            universe.Regions.Should().ContainKey("tokyo");
            universe.Regions["tokyo"].Exchange.Should().Be("binance");
            universe.Regions["tokyo"].Symbols.Should().Contain("BTCUSDT");
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Load_MissingFile_ThrowsFileNotFoundException()
    {
        string missingPath = Path.Combine(Path.GetTempPath(), $"does-not-exist-{Guid.NewGuid()}.json");

        Action act = () => TradingUniverse.Load(missingPath);

        act.Should().Throw<FileNotFoundException>();
    }

    [Fact]
    public void Load_InvalidJson_ThrowsInvalidOperationException()
    {
        string path = WriteTempUniverse("{not valid json");

        try
        {
            Action act = () => TradingUniverse.Load(path);
            act.Should().Throw<InvalidOperationException>();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Load_NoRegions_ThrowsInvalidOperationException()
    {
        string path = WriteTempUniverse("""{"version": 1, "regions": {}}""");

        try
        {
            Action act = () => TradingUniverse.Load(path);
            act.Should().Throw<InvalidOperationException>();
        }
        finally
        {
            File.Delete(path);
        }
    }
}
