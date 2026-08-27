using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.RateLimiting;

public sealed class SlidingWindowRateLimiter(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options,
    int limit = 20,
    TimeSpan? window = null) : IRateLimiter
{
    private readonly RedisOptions _opt = options.Value;
    private readonly TimeSpan _window = window ?? TimeSpan.FromMinutes(1);

    private const string Script = """
        local now = tonumber(ARGV[1])
        local windowMs = tonumber(ARGV[2])
        redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - windowMs)
        local count = redis.call('ZCARD', KEYS[1])
        if count >= tonumber(ARGV[3]) then
            local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
            local retryMs = windowMs
            if oldest[2] then retryMs = math.max(tonumber(oldest[2]) + windowMs - now, 0) end
            return {0, 0, retryMs}
        end
        redis.call('ZADD', KEYS[1], now, ARGV[4])
        redis.call('PEXPIRE', KEYS[1], windowMs)
        return {1, tonumber(ARGV[3]) - count - 1, 0}
        """;

    public async Task<RateLimitResult> AcquireAsync(string subject, CancellationToken ct = default)
    {
        string key = RedisKeys.RateSliding(_opt.KeyPrefix, subject);
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        string member = $"{now}-{Random.Shared.NextInt64():x}";

        var r = (long[])(await redis.GetDatabase().ScriptEvaluateAsync(
            Script, [(RedisKey)key],
            [now, (long)_window.TotalMilliseconds, limit, member]))!;

        return r[0] == 1
            ? RateLimitResult.Allow(r[1])
            : RateLimitResult.Block(TimeSpan.FromMilliseconds(r[2]));
    }
}
