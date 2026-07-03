using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;

namespace TradingPlatform.Common.Kafka;

/// <summary>
/// Creates Kafka topics via the Admin API. Lets a service onboard a new region by
/// config alone, without a manual infra/kafka/create-topics.sh run.
/// </summary>
public sealed partial class KafkaTopicProvisioner : IKafkaTopicProvisioner
{
    private static readonly TimeSpan _requestTimeout = TimeSpan.FromSeconds(15);

    private readonly IAdminClient _adminClient;
    private readonly ILogger<KafkaTopicProvisioner> _logger;

    /// <summary>Initializes the provisioner with an Admin API client.</summary>
    public KafkaTopicProvisioner(IAdminClient adminClient, ILogger<KafkaTopicProvisioner> logger)
    {
        _adminClient = adminClient;
        _logger      = logger;
    }

    /// <inheritdoc/>
    public async Task EnsureTopicsExistAsync(
        IReadOnlyList<TopicSpec> topics,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topics);

        if (topics.Count == 0)
        {
            return;
        }

        List<TopicSpecification> specs = topics
            .Select(t => new TopicSpecification
            {
                Name              = t.Name,
                NumPartitions     = t.Partitions,
                ReplicationFactor = t.ReplicationFactor,
            })
            .ToList();

        try
        {
            await _adminClient
                .CreateTopicsAsync(specs, new CreateTopicsOptions { RequestTimeout = _requestTimeout })
                .ConfigureAwait(false);

            foreach (TopicSpec topic in topics)
            {
                LogTopicEnsured(_logger, topic.Name);
            }
        }
        catch (CreateTopicsException ex)
        {
            List<string> unexpected = new();

            foreach (CreateTopicReport report in ex.Results)
            {
                if (report.Error.Code == ErrorCode.TopicAlreadyExists)
                {
                    LogTopicAlreadyExists(_logger, report.Topic);
                    continue;
                }

                if (report.Error.IsError)
                {
                    unexpected.Add($"{report.Topic}: {report.Error.Reason}");
                }
            }

            if (unexpected.Count > 0)
            {
                throw new InvalidOperationException(
                    $"Failed to provision Kafka topic(s): {string.Join("; ", unexpected)}", ex);
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Kafka topic ensured: {Topic}.")]
    private static partial void LogTopicEnsured(ILogger logger, string topic);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Kafka topic already exists, skipping: {Topic}.")]
    private static partial void LogTopicAlreadyExists(ILogger logger, string topic);
}
