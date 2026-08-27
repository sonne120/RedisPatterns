using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Data;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Caching;

public sealed class WriteBehindFlusher(
    IConnectionMultiplexer redis,
    IProductStore store,
    IOptions<RedisOptions> options,
    ILogger<WriteBehindFlusher> logger) : BackgroundService
{
    private const string Group = "pg-flusher";
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan ClaimIdleAfter = TimeSpan.FromSeconds(30);
    private const int BatchSize = 256;

    private readonly string _stream = RedisKeys.WriteBehindStream(options.Value.KeyPrefix);
    private readonly string _consumer = $"{Environment.MachineName}-{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await EnsureGroupAsync();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                int processed = await DrainOnceAsync();
                if (processed == 0)
                    await Task.Delay(PollInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Write-behind flush cycle failed; retrying");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
        }
    }

    private async Task<int> DrainOnceAsync()
    {
        IDatabase db = redis.GetDatabase();

        var claimed = await db.StreamAutoClaimAsync(
            _stream, Group, _consumer, (long)ClaimIdleAfter.TotalMilliseconds, "0-0", count: BatchSize);

        var fresh = await db.StreamReadGroupAsync(
            _stream, Group, _consumer, StreamPosition.NewMessages, count: BatchSize);

        StreamEntry[] batch = [.. claimed.ClaimedEntries, .. fresh];
        if (batch.Length == 0) return 0;

        var latest = new Dictionary<long, Product>();
        foreach (StreamEntry entry in batch)
        {
            string? json = entry["json"];
            if (json is not null && JsonSerializer.Deserialize<Product>(json) is { } p)
                latest[p.Id] = p;
        }

        await store.UpsertBatchAsync(latest.Values.ToArray());

        await db.StreamAcknowledgeAsync(_stream, Group, batch.Select(e => e.Id).ToArray());

        logger.LogInformation("Flushed {Rows} products ({Entries} stream entries) to Postgres",
            latest.Count, batch.Length);
        return batch.Length;
    }

    private async Task EnsureGroupAsync()
    {
        try
        {
            await redis.GetDatabase().StreamCreateConsumerGroupAsync(
                _stream, Group, StreamPosition.NewMessages, createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
        }
    }
}
