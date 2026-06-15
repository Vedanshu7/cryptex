namespace TradingPlatform.OMS.Infrastructure.Exchange;

/// <summary>Binance API credentials used by the OMS reconciliation client.</summary>
public sealed class BinanceAccountSettings
{
    /// <summary>Gets or sets the Binance API key.</summary>
    public string ApiKey { get; set; } = string.Empty;

    /// <summary>Gets or sets the Binance API secret key for HMAC signing.</summary>
    public string SecretKey { get; set; } = string.Empty;

    /// <summary>Gets or sets the Binance base URL (testnet or production).</summary>
    public Uri BaseUrl { get; set; } = new("https://testnet.binance.vision");
}
