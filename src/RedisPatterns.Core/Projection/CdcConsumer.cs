using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Idempotency;

namespace RedisPatterns.Core.Projection;

public sealed class CdcConsumer(
    ProjectionHandler handler,
    IOptions<KafkaOptions> options,
    ILogger<CdcConsumer> logger) : BackgroundService
{
    private readonly KafkaOptions _opt = options.Value;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_opt.Enabled)
        {
            logger.LogInformation("CDC projection consumer disabled by configuration");
            return Task.CompletedTask;
        }
        return Task.Factory.StartNew(
            () => RunLoop(stoppingToken), stoppingToken,
            TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    private void RunLoop(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _opt.BootstrapServers,
            GroupId = _opt.ProjectionGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => logger.LogWarning("Kafka error: {Reason}", e.Reason))
            .Build();
        consumer.Subscribe(_opt.TransactionsTopic);
        logger.LogInformation("Projecting {Topic} as group {Group}", _opt.TransactionsTopic, _opt.ProjectionGroup);

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? cr = null;
            try
            {
                cr = consumer.Consume(TimeSpan.FromSeconds(1));
                if (cr is null) continue;

                var evt = JsonSerializer.Deserialize<TransactionEvent>(cr.Message.Value);
                if (evt is null)
                {
                    logger.LogError("Malformed transaction event at {Offset}; skipping", cr.TopicPartitionOffset);
                    consumer.Commit(cr);
                    continue;
                }

                ProjectionOutcome outcome = handler.ApplyAsync(evt, stoppingToken)
                    .GetAwaiter().GetResult();
                logger.LogInformation("Projected {TxId} v{Version}: {Outcome}",
                    evt.TransactionId, evt.Version, outcome);

                consumer.Commit(cr);
            }
            catch (ConsumeException ex)
            {
                logger.LogError(ex, "Kafka consume failed");
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Projection failed at {Offset}; offset NOT committed, message will be redelivered",
                    cr?.TopicPartitionOffset);
                Thread.Sleep(1000);
            }
        }
        consumer.Close();
    }
}
