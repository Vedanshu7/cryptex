using System.Security.Cryptography;
using System.Text;

namespace TradingPlatform.EMS.Infrastructure.Exchange;

/// <summary>
/// Signs Binance API requests using HMAC-SHA256.
/// All Binance trading endpoints require a signature computed over the query string.
/// </summary>
public static class HmacSigner
{
    /// <summary>
    /// Generates an HMAC-SHA256 signature for the given query string.
    /// </summary>
    /// <param name="queryString">URL-encoded query parameters to sign.</param>
    /// <param name="secretKey">Binance API secret key.</param>
    /// <returns>Lowercase hex-encoded signature.</returns>
    public static string Sign(string queryString, string secretKey)
    {
        ArgumentNullException.ThrowIfNull(queryString);
        ArgumentNullException.ThrowIfNull(secretKey);

        byte[] keyBytes  = Encoding.UTF8.GetBytes(secretKey);
        byte[] dataBytes = Encoding.UTF8.GetBytes(queryString);

        using HMACSHA256 hmac = new(keyBytes);
        byte[] hashBytes = hmac.ComputeHash(dataBytes);

#pragma warning disable CA1308 // Binance requires lowercase hex signatures — uppercase is not accepted.
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
#pragma warning restore CA1308
    }
}
