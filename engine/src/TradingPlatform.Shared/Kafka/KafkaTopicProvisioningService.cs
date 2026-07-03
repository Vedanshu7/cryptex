using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Runs once at startup and ensures this instance's Kafka topics exist before any
/// other Kafka-dependent hosted service starts consuming or producing.
///
/// Must be registered before other hosted services that touch Kafka — the .NET
/// generic host awaits each <see cref="IHostedService.StartAsync"/> in registration
/// order, so this one blocking until done is what guarantees the ordering.
/// </summary>
public sealed partial class KafkaTopicProvisioningService : IHostedService
{
    private readonly IKafkaTopicProvisioner _provisioner;
    private readonly IReadOnlyList<TopicSpec> _topics;
    private readonly ILogger<KafkaTopicProvisioningService> _logger;

    /// <summary>Initializes the service with the topics this instance depends on.</summary>
    public KafkaTopicProvisioningService(
        IKafkaTopicProvisioner provisioner,
        IReadOnlyList<TopicSpec> topics,
        ILogger<KafkaTopicProvisioningService> logger)
    {
        _provisioner = provisioner;
        _topics      = topics;
        _logger      = logger;
    }

    /// <inheritdoc/>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        LogStarted(_logger, _topics.Count);
        await _provisioner.EnsureTopicsExistAsync(_topics, cancellationToken).ConfigureAwait(false);
        LogCompleted(_logger);
    }

    /// <inheritdoc/>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    [LoggerMessage(Level = LogLevel.Information, Message = "Provisioning {Count} Kafka topic(s) at startup.")]
    private static partial void LogStarted(ILogger logger, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Kafka topic provisioning completed.")]
    private static partial void LogCompleted(ILogger logger);
}
