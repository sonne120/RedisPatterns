using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Counters;

public sealed class CounterService(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options)
{
    private readonly RedisOptions _opt = options.Value;

    public Task<long> LikeAsync(long postId) =>
        redis.GetDatabase().StringIncrementAsync(RedisKeys.PostLikes(_opt.KeyPrefix, postId));

    public async Task<long> GetLikesAsync(long postId)
    {
        var v = await redis.GetDatabase().StringGetAsync(RedisKeys.PostLikes(_opt.KeyPrefix, postId));
        return v.IsNull ? 0 : (long)v;
    }

    public Task<long> TrackViewAsync(long postId) =>
        redis.GetDatabase().HashIncrementAsync(RedisKeys.PostViews(_opt.KeyPrefix), postId.ToString());

    public Task<bool> TrackVisitorAsync(string userId) =>
        redis.GetDatabase().HyperLogLogAddAsync(
            RedisKeys.UniqueVisitors(_opt.KeyPrefix, DateOnly.FromDateTime(DateTime.UtcNow)), userId);

    public Task<long> UniqueVisitorsTodayAsync() =>
        redis.GetDatabase().HyperLogLogLengthAsync(
            RedisKeys.UniqueVisitors(_opt.KeyPrefix, DateOnly.FromDateTime(DateTime.UtcNow)));

    public async Task HeartbeatAsync(string userId)
    {
        long minute = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 60;
        string key = RedisKeys.OnlineUsers(_opt.KeyPrefix, minute);
        IDatabase db = redis.GetDatabase();
        await db.HyperLogLogAddAsync(key, userId);
        await db.KeyExpireAsync(key, TimeSpan.FromMinutes(10));
    }

    public async Task<long> OnlineNowAsync(int windowMinutes = 5)
    {
        long minute = DateTimeOffset.UtcNow.ToUnixTimeSeconds() / 60;
        RedisKey[] buckets = Enumerable.Range(0, windowMinutes)
            .Select(i => (RedisKey)RedisKeys.OnlineUsers(_opt.KeyPrefix, minute - i))
            .ToArray();

        IDatabase db = redis.GetDatabase();
        string merged = $"{_opt.KeyPrefix}:cnt:online:merged:{Guid.NewGuid():N}";
        await db.HyperLogLogMergeAsync(merged, buckets);
        long count = await db.HyperLogLogLengthAsync(merged);
        await db.KeyDeleteAsync(merged, CommandFlags.FireAndForget);
        return count;
    }

    private const string ReserveScript = """
        -- KEYS[1] inventory key; ARGV[1] quantity requested
        local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
        local want  = tonumber(ARGV[1])
        if stock < want then
            return -1
        end
        return redis.call('DECRBY', KEYS[1], want)
        """;

    public Task SetStockAsync(long productId, long quantity) =>
        redis.GetDatabase().StringSetAsync(RedisKeys.Inventory(_opt.KeyPrefix, productId), quantity);

    public async Task<long?> TryReserveAsync(long productId, long quantity)
    {
        var result = (long)(await redis.GetDatabase().ScriptEvaluateAsync(
            ReserveScript,
            [(RedisKey)RedisKeys.Inventory(_opt.KeyPrefix, productId)],
            [quantity]))!;
        return result < 0 ? null : result;
    }
}
