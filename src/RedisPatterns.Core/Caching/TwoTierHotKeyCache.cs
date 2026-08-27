using System.Text.Json;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Caching;

public sealed class TwoTierHotKeyCache : IDisposable
{
    private static readonly TimeSpan L1Ttl = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan L2Ttl = TimeSpan.FromMinutes(30);

    private readonly IConnectionMultiplexer _redis;
    private readonly IMemoryCache _l1;
    private readonly ILogger<TwoTierHotKeyCache> _logger;
    private readonly RedisChannel _channel;

    public TwoTierHotKeyCache(
        IConnectionMultiplexer redis,
        IMemoryCache memoryCache,
        IOptions<RedisOptions> options,
        ILogger<TwoTierHotKeyCache> logger)
    {
        _redis = redis;
        _l1 = memoryCache;
        _logger = logger;
        _channel = RedisChannel.Literal(RedisKeys.InvalidationChannel(options.Value.KeyPrefix));

        _redis.GetSubscriber().Subscribe(_channel, (_, key) =>
        {
            _l1.Remove((string)key!);
            _logger.LogDebug("L1 invalidated for {Key} via pub/sub", (string)key!);
        });
    }

    public async Task<T?> GetAsync<T>(string key, Func<CancellationToken, Task<T?>> loader, CancellationToken ct = default)
        where T : class
    {
        if (_l1.TryGetValue(key, out T? l1Hit)) return l1Hit;

        var raw = await _redis.GetDatabase().StringGetAsync(key, CommandFlags.PreferReplica);
        if (!raw.IsNull)
        {
            var fromL2 = JsonSerializer.Deserialize<T>(raw.ToString());
            _l1.Set(key, fromL2, L1Ttl);
            return fromL2;
        }

        T? value = await loader(ct);
        if (value is not null)
        {
            await _redis.GetDatabase().StringSetAsync(key, JsonSerializer.Serialize(value), L2Ttl);
            _l1.Set(key, value, L1Ttl);
        }
        return value;
    }

    public async Task SetAsync<T>(string key, T value) where T : class
    {
        await _redis.GetDatabase().StringSetAsync(key, JsonSerializer.Serialize(value), L2Ttl);
        _l1.Set(key, value, L1Ttl);
        await _redis.GetSubscriber().PublishAsync(_channel, key);
    }

    public void Dispose() => _redis.GetSubscriber().Unsubscribe(_channel);
}
