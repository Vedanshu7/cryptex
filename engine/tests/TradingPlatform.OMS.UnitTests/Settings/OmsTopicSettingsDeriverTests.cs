using FluentAssertions;
using TradingPlatform.OMS.Application.Settings;

namespace TradingPlatform.OMS.UnitTests.Settings;

public sealed class OmsTopicSettingsDeriverTests
{
    [Fact]
    public void DeriveFromRegion_NoRegionSet_LeavesDefaultsUnchanged()
    {
        OmsTopicSettings settings = new();

        OmsTopicSettingsDeriver.DeriveFromRegion(settings, ["tokyo", "sgp", "eu"]);

        settings.InputTopic.Should().Be("order-requests");
        settings.OutputTopic.Should().Be("validated-orders");
        settings.FillsTopic.Should().Be("order-fills");
    }

    [Theory]
    [InlineData("tokyo")]
    [InlineData("sgp")]
    [InlineData("eu")]
    public void DeriveFromRegion_RegionSet_DerivesAllThreeTopics(string region)
    {
        OmsTopicSettings settings = new() { Region = region };

        OmsTopicSettingsDeriver.DeriveFromRegion(settings, ["tokyo", "sgp", "eu"]);

        settings.InputTopic.Should().Be($"{region}.order-requests");
        settings.OutputTopic.Should().Be($"{region}.validated-orders");
        settings.FillsTopic.Should().Be($"{region}.order-fills");
    }

    [Fact]
    public void DeriveFromRegion_ExplicitOverridePreserved()
    {
        OmsTopicSettings settings = new()
        {
            Region = "tokyo",
            InputTopic = "custom-input-topic",
        };

        OmsTopicSettingsDeriver.DeriveFromRegion(settings, ["tokyo"]);

        settings.InputTopic.Should().Be("custom-input-topic");
        settings.OutputTopic.Should().Be("tokyo.validated-orders");
        settings.FillsTopic.Should().Be("tokyo.order-fills");
    }

    [Fact]
    public void DeriveFromRegion_UnknownRegion_ThrowsInvalidOperationException()
    {
        OmsTopicSettings settings = new() { Region = "atlantis" };

        Action act = () => OmsTopicSettingsDeriver.DeriveFromRegion(settings, ["tokyo", "sgp", "eu"]);

        act.Should().Throw<InvalidOperationException>().WithMessage("*atlantis*");
    }
}
