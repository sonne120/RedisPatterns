using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Locking;

public sealed class DistributedLockFactory(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options,
    ILogger<DistributedLockFactory> logger)
{
    private readonly RedisOptions _opt = options.Value;

    public async Task<DistributedLockHandle?> TryAcquireAsync(
        string resource,
        TimeSpan? ttl = null,
        bool autoRenew = true)
    {
        ttl ??= TimeSpan.FromSeconds(30);
        string key = RedisKeys.Lock(_opt.KeyPrefix, resource);
        string token = Guid.NewGuid().ToString("N");

        bool acquired = await redis.GetDatabase()
            .StringSetAsync(key, token, expiry: ttl, when: When.NotExists);
        if (!acquired) return null;

        return new DistributedLockHandle(redis, logger, key, token, ttl.Value, autoRenew);
    }

    public async Task<DistributedLockHandle?> AcquireAsync(
        string resource,
        TimeSpan waitTimeout,
        TimeSpan? ttl = null,
        CancellationToken ct = default)
    {
        var deadline = DateTime.UtcNow + waitTimeout;
        var delay = TimeSpan.FromMilliseconds(20);

        while (true)
        {
            var handle = await TryAcquireAsync(resource, ttl);
            if (handle is not null) return handle;
            if (DateTime.UtcNow + delay > deadline) return null;

            await Task.Delay(delay + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 20)), ct);
            delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, 500));
        }
    }
}

public sealed class DistributedLockHandle : IAsyncDisposable
{
    private const string ReleaseScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        """;

    private const string RenewScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        end
        return 0
        """;

    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger _logger;
    private readonly string _key;
    private readonly string _token;
    private readonly TimeSpan _ttl;
    private readonly CancellationTokenSource _renewCts = new();

    public bool IsHeld { get; private set; } = true;

    internal DistributedLockHandle(
        IConnectionMultiplexer redis, ILogger logger,
        string key, string token, TimeSpan ttl, bool autoRenew)
    {
        _redis = redis;
        _logger = logger;
        _key = key;
        _token = token;
        _ttl = ttl;
        if (autoRenew)
            _ = RenewLoopAsync(_renewCts.Token);
    }

    private async Task RenewLoopAsync(CancellationToken ct)
    {
        var period = TimeSpan.FromMilliseconds(_ttl.TotalMilliseconds / 3);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(period, ct);
                var result = await _redis.GetDatabase().ScriptEvaluateAsync(
                    RenewScript, [(RedisKey)_key], [(RedisValue)_token, (long)_ttl.TotalMilliseconds]);
                if ((long)result == 0)
                {
                    IsHeld = false;
                    _logger.LogWarning("Lost distributed lock {Key} — renewal found another owner", _key);
                    return;
                }
            }
        }
        catch (OperationCanceledException) {  }
        catch (Exception ex)
        {
            IsHeld = false;
            _logger.LogError(ex, "Lock renewal for {Key} failed", _key);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _renewCts.CancelAsync();
        _renewCts.Dispose();
        try
        {
            await _redis.GetDatabase().ScriptEvaluateAsync(
                ReleaseScript, [(RedisKey)_key], [(RedisValue)_token]);
        }
        catch (RedisException ex)
        {
            _logger.LogWarning(ex, "Failed to release lock {Key}; it will expire by TTL", _key);
        }
        IsHeld = false;
    }
}
