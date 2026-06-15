using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Web;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TradingPlatform.OMS.Domain.Interfaces;

namespace TradingPlatform.OMS.Infrastructure.Exchange;

/// <summary>
/// Queries Binance for individual order status using the origClientOrderId
/// set by the EMS at placement time. Used only by the reconciliation service.
/// </summary>
public sealed partial class BinanceAccountClient : IBinanceAccountClient
{
    private readonly HttpClient _http;
    private readonly BinanceAccountSettings _settings;
    private readonly ILogger<BinanceAccountClient> _logger;

    /// <summary>Initializes the client.</summary>
    public BinanceAccountClient(
        HttpClient http,
        IOptions<BinanceAccountSettings> settings,
        ILogger<BinanceAccountClient> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        _http     = http;
        _settings = settings.Value;
        _logger   = logger;
    }

    /// <inheritdoc/>
    public async Task<BinanceOrderQueryResult?> GetOrderByClientIdAsync(
        string symbol,
        string clientOrderId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(symbol);
        ArgumentNullException.ThrowIfNull(clientOrderId);

        long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        string queryString = string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"symbol={HttpUtility.UrlEncode(symbol)}" +
            $"&origClientOrderId={HttpUtility.UrlEncode(clientOrderId)}" +
            $"&timestamp={timestamp}");

        string signature = _Sign(queryString, _settings.SecretKey);
        string url       = $"/api/v3/order?{queryString}&signature={signature}";

        using HttpRequestMessage request = new(HttpMethod.Get, url);
        request.Headers.Add("X-MBX-APIKEY", _settings.ApiKey);

        HttpResponseMessage response = await _http
            .SendAsync(request, ct)
            .ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            // Order was never placed on Binance (EMS crashed before sending).
            LogOrderNotFound(_logger, clientOrderId, symbol);
            return null;
        }

        if (!response.IsSuccessStatusCode)
        {
            string body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            LogQueryError(_logger, (int)response.StatusCode, body);
            return null;
        }

        BinanceOrderResponse? raw = await response.Content
            .ReadFromJsonAsync<BinanceOrderResponse>(
                cancellationToken: ct)
            .ConfigureAwait(false);

        if (raw is null)
        {
            return null;
        }

        return new BinanceOrderQueryResult
        {
            Status                = raw.Status,
            ExecutedQty           = raw.ExecutedQty,
            CummulativeQuoteQty   = raw.CummulativeQuoteQty,
        };
    }

    private static string _Sign(string queryString, string secretKey)
    {
        byte[] keyBytes  = Encoding.UTF8.GetBytes(secretKey);
        byte[] dataBytes = Encoding.UTF8.GetBytes(queryString);
        using HMACSHA256 hmac = new(keyBytes);
        byte[] hash = hmac.ComputeHash(dataBytes);
#pragma warning disable CA1308 // Binance requires lowercase hex signatures.
        return Convert.ToHexString(hash).ToLowerInvariant();
#pragma warning restore CA1308
    }

    [LoggerMessage(Level = LogLevel.Debug,
        Message = "Order {ClientOrderId} for {Symbol} not found on Binance — was never placed.")]
    private static partial void LogOrderNotFound(ILogger logger, string clientOrderId, string symbol);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Binance order query failed with {StatusCode}: {Body}.")]
    private static partial void LogQueryError(ILogger logger, int statusCode, string body);

    // Matches Binance GET /api/v3/order response shape.
#pragma warning disable CA1812
    private sealed class BinanceOrderResponse
    {
        public string  Status              { get; set; } = string.Empty;
        public decimal ExecutedQty         { get; set; }
        public decimal CummulativeQuoteQty { get; set; }
    }
#pragma warning restore CA1812
}
