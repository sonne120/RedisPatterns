using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace RedisPatterns.Core.Idempotency;

public sealed class OrderEventsConsumer(
    IdempotencyGuard idempotency,
    IOptions<KafkaOptions> options,
    ILogger<OrderEventsConsumer> logger) : BackgroundService
{
    private readonly KafkaOptions _opt = options.Value;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_opt.Enabled)
        {
            logger.LogInformation("Kafka consumer disabled by configuration");
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
            GroupId = _opt.ConsumerGroup,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnablePartitionEof = false,
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => logger.LogWarning("Kafka error: {Reason}", e.Reason))
            .Build();
        consumer.Subscribe(_opt.OrdersTopic);
        logger.LogInformation("Consuming {Topic} as group {Group}", _opt.OrdersTopic, _opt.ConsumerGroup);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var cr = consumer.Consume(TimeSpan.FromSeconds(1));
                if (cr is null) continue;

                HandleAsync(cr).GetAwaiter().GetResult();
                consumer.Commit(cr);
            }
            catch (ConsumeException ex)
            {
                logger.LogError(ex, "Kafka consume failed");
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "Order processing failed; offset not committed");
            }
        }
        consumer.Close();
    }

    private async Task HandleAsync(ConsumeResult<string, string> cr)
    {
        string eventId = cr.Message.Key
            ?? throw new InvalidOperationException("Order events must carry the event id as the message key.");

        var check = await idempotency.TryBeginAsync($"order:{eventId}");
        switch (check.Decision)
        {
            case IdempotencyDecision.AlreadyDone:
                logger.LogInformation("Duplicate order event {EventId} skipped (dedup hit)", eventId);
                return;

            case IdempotencyDecision.InProgress:
                logger.LogInformation("Order event {EventId} already in progress elsewhere", eventId);
                return;

            case IdempotencyDecision.Execute:
                try
                {
                    logger.LogInformation("Processing order event {EventId}: {Payload}",
                        eventId, cr.Message.Value);
                    await Task.Delay(50);
                    await idempotency.CompleteAsync($"order:{eventId}", "processed");
                }
                catch
                {
                    await idempotency.FailAsync($"order:{eventId}");
                    throw;
                }
                return;
        }
    }
}
