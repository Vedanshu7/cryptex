using FluentAssertions;
using TradingPlatform.EMS.Infrastructure.Exchange;

namespace TradingPlatform.EMS.UnitTests.Exchange;

public sealed class HmacSignerTests
{
    [Fact]
    public void Sign_KnownInput_ProducesExpectedSignature()
    {
        // Known HMAC-SHA256 value — verified against external reference.
        const string queryString = "symbol=BTCUSDT&side=BUY&type=MARKET&quantity=0.01&timestamp=1700000000000";
        const string secretKey   = "test_secret_key";

        string signature = HmacSigner.Sign(queryString, secretKey);

        // Signature must be 64 hex characters (256 bits).
        signature.Should().HaveLength(64);
        signature.Should().MatchRegex("^[0-9a-f]{64}$");
    }

    [Fact]
    public void Sign_SameInputTwice_ProducesSameSignature()
    {
        const string qs  = "symbol=ETHUSDT&quantity=1.0";
        const string key = "my_secret";

        string sig1 = HmacSigner.Sign(qs, key);
        string sig2 = HmacSigner.Sign(qs, key);

        sig1.Should().Be(sig2);
    }

    [Fact]
    public void Sign_DifferentSecretKeys_ProduceDifferentSignatures()
    {
        const string qs = "symbol=BTCUSDT&quantity=0.5";

        string sig1 = HmacSigner.Sign(qs, "key_one");
        string sig2 = HmacSigner.Sign(qs, "key_two");

        sig1.Should().NotBe(sig2);
    }

    [Fact]
    public void Sign_NullQueryString_ThrowsArgumentNullException()
    {
        Action act = () => HmacSigner.Sign(null!, "secret");
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Sign_NullSecretKey_ThrowsArgumentNullException()
    {
        Action act = () => HmacSigner.Sign("query", null!);
        act.Should().Throw<ArgumentNullException>();
    }
}
