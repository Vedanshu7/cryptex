using System.Text.Json;

namespace TradingPlatform.Common.Universe;

/// <summary>
/// A versioned snapshot of each region's exchange and tradable symbols.
/// Mirrors pipeline/shared/universe.py's Universe — read once at OMS/EMS
/// startup (unlike the Python side, which polls for hot-reload) since a
/// deployed OMS/EMS instance's region is fixed for its process lifetime.
/// </summary>
public sealed record TradingUniverse
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>Universe schema version.</summary>
    public required int Version { get; init; }

    /// <summary>Region name (e.g. "tokyo") to its exchange/symbol assignment.</summary>
    public required IReadOnlyDictionary<string, RegionUniverse> Regions { get; init; }

    /// <summary>
    /// Loads and parses a universe.json file from disk.
    /// </summary>
    /// <exception cref="FileNotFoundException">The file does not exist.</exception>
    /// <exception cref="InvalidOperationException">The file is not valid JSON for this schema.</exception>
    public static TradingUniverse Load(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Universe file not found: {path}", path);
        }

        string json = File.ReadAllText(path);

        TradingUniverse? universe;
        try
        {
            universe = JsonSerializer.Deserialize<TradingUniverse>(json, JsonOptions);
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"Universe file at {path} is not valid JSON.", ex);
        }

        if (universe is null || universe.Regions.Count == 0)
        {
            throw new InvalidOperationException($"Universe file at {path} declares no regions.");
        }

        return universe;
    }
}
