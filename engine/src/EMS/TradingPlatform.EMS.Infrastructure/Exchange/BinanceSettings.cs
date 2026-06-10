namespace TradingPlatform.EMS.Infrastructure.Exchange;

/// <summary>Binance API configuration bound from the Binance configuration section.</summary>
public sealed record BinanceSettings
{
    /// <summary>Gets the base URL (use testnet URL for local development).</summary>
    public required Uri BaseUrl { get; init; }

    /// <summary>Gets the Binance API key.</summary>
    public required string ApiKey { get; init; }

    /// <summary>Gets the Binance API secret key (used for HMAC signing).</summary>
    public required string SecretKey { get; init; }
}
