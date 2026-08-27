using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Messaging;

public sealed class DelayedJobScheduler(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options)
{
    private readonly RedisOptions _opt = options.Value;

    public async Task ScheduleAsync(string type, string payload, TimeSpan delay)
    {
        var job = new JobEnvelope(Guid.NewGuid().ToString("N"), type, payload);
        double dueAt = DateTimeOffset.UtcNow.Add(delay).ToUnixTimeMilliseconds();
        await redis.GetDatabase().SortedSetAddAsync(
            RedisKeys.DelayedJobs(_opt.KeyPrefix), JsonSerializer.Serialize(job), dueAt);
    }

    public async Task<long> DueCountAsync() =>
        await redis.GetDatabase().SortedSetLengthAsync(RedisKeys.DelayedJobs(_opt.KeyPrefix));

    private const string PromoteScript = """
        -- KEYS[1] delayed zset, KEYS[2] job stream
        -- ARGV[1] now ms, ARGV[2] max batch
        local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
        for _, jobJson in ipairs(due) do
            redis.call('XADD', KEYS[2], '*', 'job', jobJson)
            redis.call('ZREM', KEYS[1], jobJson)
        end
        return #due
        """;

    public async Task<long> PromoteDueAsync(int maxBatch = 100)
    {
        var result = await redis.GetDatabase().ScriptEvaluateAsync(
            PromoteScript,
            [(RedisKey)RedisKeys.DelayedJobs(_opt.KeyPrefix), (RedisKey)RedisKeys.JobStream(_opt.KeyPrefix)],
            [DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), maxBatch]);
        return (long)result;
    }
}

public sealed class DelayedJobPromoter(
    DelayedJobScheduler scheduler,
    ILogger<DelayedJobPromoter> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                long promoted = await scheduler.PromoteDueAsync();
                if (promoted > 0)
                    logger.LogInformation("Promoted {Count} delayed job(s) to the work stream", promoted);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Delayed-job promotion failed; will retry next tick");
            }
        }
    }
}
