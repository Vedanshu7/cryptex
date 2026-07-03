using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using TradingPlatform.Common.Kafka;

namespace TradingPlatform.OMS.UnitTests.Kafka;

public sealed class KafkaTopicProvisionerTests
{
    private readonly Mock<IAdminClient> _adminClient = new();

    private KafkaTopicProvisioner CreateProvisioner() =>
        new(_adminClient.Object, NullLogger<KafkaTopicProvisioner>.Instance);

    [Fact]
    public async Task EnsureTopicsExistAsync_TopicsDoNotExist_CreatesThemViaAdminClient()
    {
        _adminClient
            .Setup(a => a.CreateTopicsAsync(
                It.IsAny<IEnumerable<TopicSpecification>>(),
                It.IsAny<CreateTopicsOptions>()))
            .Returns(Task.CompletedTask);

        List<TopicSpec> topics =
        [
            new() { Name = "tokyo.order-requests" },
            new() { Name = "tokyo.validated-orders" },
        ];

        await CreateProvisioner().EnsureTopicsExistAsync(topics);

        string[] expectedNames = ["tokyo.order-requests", "tokyo.validated-orders"];

        _adminClient.Verify(
            a => a.CreateTopicsAsync(
                It.Is<IEnumerable<TopicSpecification>>(specs =>
                    specs.Select(s => s.Name).SequenceEqual(expectedNames)),
                It.IsAny<CreateTopicsOptions>()),
            Times.Once);
    }

    [Fact]
    public async Task EnsureTopicsExistAsync_TopicAlreadyExists_TreatsAsSuccess()
    {
        CreateTopicsException alreadyExists = new(
        [
            new CreateTopicReport
            {
                Topic = "tokyo.order-requests",
                Error = new Error(ErrorCode.TopicAlreadyExists),
            },
        ]);

        _adminClient
            .Setup(a => a.CreateTopicsAsync(
                It.IsAny<IEnumerable<TopicSpecification>>(),
                It.IsAny<CreateTopicsOptions>()))
            .ThrowsAsync(alreadyExists);

        List<TopicSpec> topics = [new() { Name = "tokyo.order-requests" }];

        Func<Task> act = () => CreateProvisioner().EnsureTopicsExistAsync(topics);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task EnsureTopicsExistAsync_UnexpectedBrokerError_ThrowsInvalidOperationException()
    {
        CreateTopicsException invalidConfig = new(
        [
            new CreateTopicReport
            {
                Topic = "tokyo.order-requests",
                Error = new Error(ErrorCode.InvalidConfig, "replication factor larger than available brokers"),
            },
        ]);

        _adminClient
            .Setup(a => a.CreateTopicsAsync(
                It.IsAny<IEnumerable<TopicSpecification>>(),
                It.IsAny<CreateTopicsOptions>()))
            .ThrowsAsync(invalidConfig);

        List<TopicSpec> topics = [new() { Name = "tokyo.order-requests" }];

        Func<Task> act = () => CreateProvisioner().EnsureTopicsExistAsync(topics);

        (await act.Should().ThrowAsync<InvalidOperationException>())
            .WithMessage("*tokyo.order-requests*");
    }

    [Fact]
    public async Task EnsureTopicsExistAsync_EmptyList_DoesNotCallAdminClient()
    {
        await CreateProvisioner().EnsureTopicsExistAsync([]);

        _adminClient.Verify(
            a => a.CreateTopicsAsync(
                It.IsAny<IEnumerable<TopicSpecification>>(),
                It.IsAny<CreateTopicsOptions>()),
            Times.Never);
    }
}
