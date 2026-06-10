using System.Net.Http.Json;
using System.Text.Json;
using System.Web;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TradingPlatform.EMS.Domain.Interfaces;

namespace TradingPlatform.EMS.Infrastructure.Exchange;

/// <summary>
/// Binance REST API client with HMAC-SHA256 request signing.
/// Targets the testnet endpoint in local development.
/// </summary>
public sealed partial class BinanceClient : IBinanceClient
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly HttpClient _http;
    private readonly BinanceSettings _settings;
    private readonly ILogger<BinanceClient> _logger;

    /// <summary>Initializes the client with configured options.</summary>
    public BinanceClient(
        HttpClient http,
        IOptions<BinanceSettings> settings,
        ILogger<BinanceClient> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        _http     = http;
        _settings = settings.Value;
        _logger   = logger;
    }

    /// <inheritdoc/>
    public async Task<BinanceFillResult> PlaceMarketOrderAsync(
        string symbol,
        string side,
        decimal quantity,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(symbol);
        ArgumentNullException.ThrowIfNull(side);

        long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        string queryString = string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"symbol={HttpUtility.UrlEncode(symbol)}&side={side.ToUpperInvariant()}" +
            $"&type=MARKET&quantity={quantity}&timestamp={timestamp}");

        string signature = HmacSigner.Sign(queryString, _settings.SecretKey);
        string url = $"/api/v3/order?{queryString}&signature={signature}";

        using HttpRequestMessage request = new(HttpMethod.Post, url);
        request.Headers.Add("X-MBX-APIKEY", _settings.ApiKey);

        HttpResponseMessage response = await _http
            .SendAsync(request, ct)
            .ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            string body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            LogBinanceError(_logger, (int)response.StatusCode, body);
            throw new InvalidOperationException(
                $"Binance order rejected: {response.StatusCode} — {body}");
        }

        BinanceOrderResponse? result = await response.Content
            .ReadFromJsonAsync<BinanceOrderResponse>(JsonOptions, ct)
            .ConfigureAwait(false);

        if (result is null)
        {
            throw new InvalidOperationException("Binance returned an empty response body.");
        }

        decimal fillPrice = result.Fills is { Count: > 0 }
            ? result.Fills.Average(f => f.Price)
            : result.Price;

        LogOrderPlaced(_logger, result.OrderId, fillPrice);

        return new BinanceFillResult
        {
            OrderId   = result.OrderId,
            FillPrice = fillPrice,
        };
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Binance error {StatusCode}: {Body}.")]
    private static partial void LogBinanceError(ILogger logger, int statusCode, string body);

    [LoggerMessage(Level = LogLevel.Information, Message = "Binance order {ExchangeOrderId} filled at {FillPrice}.")]
    private static partial void LogOrderPlaced(ILogger logger, long exchangeOrderId, decimal fillPrice);

    // Response shape matches Binance /api/v3/order POST response.
    // Instantiated at runtime by System.Text.Json — suppress CA1812.
#pragma warning disable CA1812
    private sealed class BinanceOrderResponse
    {
        public long OrderId { get; set; }
        public decimal Price { get; set; }
        public List<FillEntry>? Fills { get; set; }
    }

    private sealed class FillEntry
    {
        public decimal Price { get; set; }
    }
#pragma warning restore CA1812
}
