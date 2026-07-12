using FluentAssertions;
using TradingPlatform.EMS.Application.Settings;

namespace TradingPlatform.EMS.UnitTests.Settings;

public sealed class EmsTopicSettingsDeriverTests
{
    [Fact]
    public void DeriveFromRegion_NoRegionSet_LeavesDefaultsUnchanged()
    {
        EmsTopicSettings settings = new();

        EmsTopicSettingsDeriver.DeriveFromRegion(settings, ["tokyo", "sgp", "eu"]);

        settings.InputTopic.Should().Be("validated-orders");
        settings.OutputTopic.Should().Be("order-fills");
    }

    [Theory]
    [InlineData("tokyo")]
    [InlineData("sgp")]
    [InlineData("eu")]
    public void DeriveFromRegion_RegionSet_DerivesBothTopics(string region)
    {
        EmsTopicSettings settings = new() { Region = region };

        EmsTopicSettingsDeriver.DeriveFromRegion(settings, ["tokyo", "sgp", "eu"]);

        settings.InputTopic.Should().Be($"{region}.validated-orders");
        settings.OutputTopic.Should().Be($"{region}.order-fills");
    }

    [Fact]
    public void DeriveFromRegion_ExplicitOverridePreserved()
    {
        EmsTopicSettings settings = new()
        {
            Region = "tokyo",
            OutputTopic = "custom-output-topic",
        };

        EmsTopicSettingsDeriver.DeriveFromRegion(settings, ["tokyo"]);

        settings.InputTopic.Should().Be("tokyo.validated-orders");
        settings.OutputTopic.Should().Be("custom-output-topic");
    }

    [Fact]
    public void DeriveFromRegion_UnknownRegion_ThrowsInvalidOperationException()
    {
        EmsTopicSettings settings = new() { Region = "atlantis" };

        Action act = () => EmsTopicSettingsDeriver.DeriveFromRegion(settings, ["tokyo", "sgp", "eu"]);

        act.Should().Throw<InvalidOperationException>().WithMessage("*atlantis*");
    }
}
