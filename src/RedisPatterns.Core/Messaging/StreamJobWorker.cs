using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace RedisPatterns.Core.Messaging;

public sealed class StreamJobWorker(
    IConnectionMultiplexer redis,
    StreamJobQueue queue,
    ILogger<StreamJobWorker> logger) : BackgroundService
{
    private const int MaxAttempts = 5;
    private static readonly TimeSpan ClaimIdleAfter = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan PollDelay = TimeSpan.FromMilliseconds(500);

    private readonly string _consumer = $"{Environment.MachineName}-{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await queue.EnsureGroupAsync();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                int handled = await ConsumeOnceAsync();
                if (handled == 0)
                    await Task.Delay(PollDelay, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "Worker cycle failed; retrying");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }

    private async Task<int> ConsumeOnceAsync()
    {
        IDatabase db = redis.GetDatabase();

        var claimed = await db.StreamAutoClaimAsync(
            queue.StreamKey, StreamJobQueue.ConsumerGroup, _consumer,
            (long)ClaimIdleAfter.TotalMilliseconds, "0-0", count: 16);

        var fresh = await db.StreamReadGroupAsync(
            queue.StreamKey, StreamJobQueue.ConsumerGroup, _consumer,
            StreamPosition.NewMessages, count: 16);

        StreamEntry[] batch = [.. claimed.ClaimedEntries, .. fresh];
        foreach (StreamEntry entry in batch)
            await HandleEntryAsync(db, entry);
        return batch.Length;
    }

    private async Task HandleEntryAsync(IDatabase db, StreamEntry entry)
    {
        JobEnvelope? job = null;
        try
        {
            string? raw = entry["job"];
            job = raw is null ? null : JsonSerializer.Deserialize<JobEnvelope>(raw);
            if (job is null)
                throw new InvalidOperationException($"Malformed job entry {entry.Id}");

            long deliveries = await DeliveryCountAsync(db, entry.Id);
            if (deliveries > MaxAttempts)
            {
                await DeadLetterAsync(db, entry, $"exceeded {MaxAttempts} attempts");
                return;
            }

            await ProcessAsync(job);

            await db.StreamAcknowledgeAsync(queue.StreamKey, StreamJobQueue.ConsumerGroup, entry.Id);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Job {JobId} (entry {EntryId}) failed; will be retried",
                job?.JobId ?? "?", entry.Id);
        }
    }

    private async Task<long> DeliveryCountAsync(IDatabase db, RedisValue entryId)
    {
        StreamPendingMessageInfo[] info = await db.StreamPendingMessagesAsync(
            queue.StreamKey, StreamJobQueue.ConsumerGroup,
            count: 1, consumerName: _consumer, minId: entryId, maxId: entryId);
        return info.Length > 0 ? info[0].DeliveryCount : 1;
    }

    private async Task DeadLetterAsync(IDatabase db, StreamEntry entry, string reason)
    {
        await db.StreamAddAsync(queue.StreamKey + ":dead",
            [new NameValueEntry("origin-id", entry.Id.ToString()),
             new NameValueEntry("job", entry["job"]),
             new NameValueEntry("reason", reason)]);
        await db.StreamAcknowledgeAsync(queue.StreamKey, StreamJobQueue.ConsumerGroup, entry.Id);
        logger.LogError("Job entry {EntryId} dead-lettered: {Reason}", entry.Id, reason);
    }

    private async Task ProcessAsync(JobEnvelope job)
    {
        if (job.Type == "fail")
            throw new InvalidOperationException("This job always fails (demo of retries + DLQ)");

        logger.LogInformation("Processing job {JobId} of type {Type}: {Payload}",
            job.JobId, job.Type, job.Payload);
        await Task.Delay(100);
    }
}
