namespace TradingPlatform.Common.Universe;

/// <summary>
/// The exchange and tradable symbols assigned to one region.
/// Mirrors pipeline/shared/universe.py's RegionUniverse.
/// </summary>
public sealed record RegionUniverse
{
    /// <summary>Exchange this region routes orders to (e.g. "binance").</summary>
    public required string Exchange { get; init; }

    /// <summary>Symbols tradable on this region's exchange, in that exchange's own notation.</summary>
    public required IReadOnlyList<string> Symbols { get; init; }
}
