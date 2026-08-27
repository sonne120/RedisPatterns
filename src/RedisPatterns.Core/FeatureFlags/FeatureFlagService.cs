using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.FeatureFlags;

public sealed class FeatureFlagService : IDisposable
{
    private static readonly TimeSpan RefreshInterval = TimeSpan.FromSeconds(30);

    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<FeatureFlagService> _logger;
    private readonly string _hashKey;
    private readonly RedisChannel _channel;
    private readonly Timer _refreshTimer;

    private volatile ConcurrentDictionary<string, string> _snapshot = new();

    public FeatureFlagService(
        IConnectionMultiplexer redis,
        IOptions<RedisOptions> options,
        ILogger<FeatureFlagService> logger)
    {
        _redis = redis;
        _logger = logger;
        _hashKey = RedisKeys.FeatureFlags(options.Value.KeyPrefix);
        _channel = RedisChannel.Literal(RedisKeys.FlagsChangedChannel(options.Value.KeyPrefix));

        _redis.GetSubscriber().Subscribe(_channel, (_, _) => _ = RefreshAsync());
        _refreshTimer = new Timer(_ => _ = RefreshAsync(), null, TimeSpan.Zero, RefreshInterval);
    }

    public bool IsEnabled(string flag, bool defaultValue = false) =>
        _snapshot.TryGetValue(flag, out var v)
            ? v is "1" or "true" or "on"
            : defaultValue;

    public string? GetValue(string flag) =>
        _snapshot.TryGetValue(flag, out var v) ? v : null;

    public IReadOnlyDictionary<string, string> All() => _snapshot;

    public async Task SetAsync(string flag, string value)
    {
        IDatabase db = _redis.GetDatabase();
        await db.HashSetAsync(_hashKey, flag, value);
        await _redis.GetSubscriber().PublishAsync(_channel, flag);
    }

    private async Task RefreshAsync()
    {
        try
        {
            HashEntry[] entries = await _redis.GetDatabase().HashGetAllAsync(_hashKey);
            _snapshot = new ConcurrentDictionary<string, string>(
                entries.Select(e => new KeyValuePair<string, string>(e.Name.ToString(), e.Value.ToString())));
        }
        catch (RedisException ex)
        {
            _logger.LogWarning(ex, "Feature flag refresh failed; serving last known snapshot");
        }
    }

    public void Dispose()
    {
        _refreshTimer.Dispose();
        _redis.GetSubscriber().Unsubscribe(_channel);
    }
}
