using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.RateLimiting;

public sealed class FixedWindowRateLimiter(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options,
    int limit = 100,
    TimeSpan? window = null) : IRateLimiter
{
    private readonly RedisOptions _opt = options.Value;
    private readonly TimeSpan _window = window ?? TimeSpan.FromMinutes(1);

    private const string Script = """
        local count = redis.call('INCR', KEYS[1])
        if count == 1 then
            redis.call('PEXPIRE', KEYS[1], ARGV[2])
        end
        if count > tonumber(ARGV[1]) then
            return {0, 0, redis.call('PTTL', KEYS[1])}
        end
        return {1, tonumber(ARGV[1]) - count, 0}
        """;

    public async Task<RateLimitResult> AcquireAsync(string subject, CancellationToken ct = default)
    {
        long windowMs = (long)_window.TotalMilliseconds;
        long windowStamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / windowMs;
        string key = RedisKeys.RateFixed(_opt.KeyPrefix, subject, windowStamp);

        var r = (long[])(await redis.GetDatabase().ScriptEvaluateAsync(
            Script, [(RedisKey)key], [limit, windowMs]))!;

        return r[0] == 1
            ? RateLimitResult.Allow(r[1])
            : RateLimitResult.Block(TimeSpan.FromMilliseconds(Math.Max(r[2], 0)));
    }
}
