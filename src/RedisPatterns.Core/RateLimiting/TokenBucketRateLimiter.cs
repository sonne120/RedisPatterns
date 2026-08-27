using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.RateLimiting;

public sealed class TokenBucketRateLimiter(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options,
    int capacity = 10,
    double refillRatePerSecond = 5.0) : IRateLimiter
{
    private readonly RedisOptions _opt = options.Value;

    private const string Script = """
        local capacity = tonumber(ARGV[1])
        local rate     = tonumber(ARGV[2])
        local cost     = tonumber(ARGV[3])

        local t = redis.call('TIME')
        local nowMs = t[1] * 1000 + math.floor(t[2] / 1000)

        local state  = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
        local tokens = tonumber(state[1])
        local ts     = tonumber(state[2])
        if tokens == nil then tokens = capacity; ts = nowMs end

        tokens = math.min(capacity, tokens + (nowMs - ts) / 1000.0 * rate)

        local allowed = 0
        local retryMs = 0
        if tokens >= cost then
            tokens = tokens - cost
            allowed = 1
        else
            retryMs = math.ceil((cost - tokens) / rate * 1000)
        end

        redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', nowMs)
        redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) + 60000)
        return {allowed, math.floor(tokens), retryMs}
        """;

    public async Task<RateLimitResult> AcquireAsync(string subject, CancellationToken ct = default)
    {
        string key = RedisKeys.RateBucket(_opt.KeyPrefix, subject);
        var r = (long[])(await redis.GetDatabase().ScriptEvaluateAsync(
            Script, [(RedisKey)key], [capacity, refillRatePerSecond, 1]))!;

        return r[0] == 1
            ? RateLimitResult.Allow(r[1])
            : RateLimitResult.Block(TimeSpan.FromMilliseconds(r[2]));
    }
}
