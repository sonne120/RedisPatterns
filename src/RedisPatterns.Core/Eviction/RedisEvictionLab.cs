using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Eviction;

public sealed record EvictionPolicyInfo(string MaxMemoryPolicy, string MaxMemory, int MaxMemorySamples);
public sealed record KeyInspection(string Key, bool Exists, long? IdleTimeSeconds, long? LfuFrequency, string? Note);
public sealed record EvictionStats(long EvictedKeys, long ExpiredKeys, long KeyspaceHits, long KeyspaceMisses, double HitRate, long UsedMemoryBytes);

public sealed class RedisEvictionLab(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options)
{
    private readonly RedisOptions _opt = options.Value;

    private IServer Server => redis.GetServers().First(s => !s.IsReplica);

    public async Task<EvictionPolicyInfo> GetPolicyAsync()
    {
        KeyValuePair<string, string>[] policy = await Server.ConfigGetAsync("maxmemory-policy");
        KeyValuePair<string, string>[] maxmem = await Server.ConfigGetAsync("maxmemory");
        KeyValuePair<string, string>[] samples = await Server.ConfigGetAsync("maxmemory-samples");
        return new EvictionPolicyInfo(
            policy[0].Value, maxmem[0].Value, int.Parse(samples[0].Value));
    }

    private static readonly HashSet<string> ValidPolicies =
    [
        "noeviction",
        "allkeys-lru", "volatile-lru",
        "allkeys-lfu", "volatile-lfu",
        "allkeys-random", "volatile-random",
        "volatile-ttl"
    ];

    public async Task SetPolicyAsync(string policy, string? maxMemory = null)
    {
        if (!ValidPolicies.Contains(policy))
            throw new ArgumentException($"Unknown policy '{policy}'. Valid: {string.Join(", ", ValidPolicies)}");
        await Server.ConfigSetAsync("maxmemory-policy", policy);
        if (maxMemory is not null)
            await Server.ConfigSetAsync("maxmemory", maxMemory);
    }

    public async Task SeedAsync(int count, int startAt = 0)
    {
        IDatabase db = redis.GetDatabase();
        string payload = new('x', 1024);
        foreach (int[] chunk in Enumerable.Range(startAt, count).Chunk(500))
        {
            IBatch batch = db.CreateBatch();
            var pending = chunk
                .Select(i => batch.StringSetAsync(
                    RedisKeys.EvictionKey(_opt.KeyPrefix, i), payload, TimeSpan.FromHours(1)))
                .ToArray();
            batch.Execute();
            await Task.WhenAll(pending);
        }
    }

    public async Task HeatAsync(int hotCount, int rounds = 10)
    {
        IDatabase db = redis.GetDatabase();
        for (int round = 0; round < rounds; round++)
        {
            IBatch batch = db.CreateBatch();
            var pending = Enumerable.Range(0, hotCount)
                .Select(i => batch.StringGetAsync(RedisKeys.EvictionKey(_opt.KeyPrefix, i)))
                .ToArray();
            batch.Execute();
            await Task.WhenAll(pending);
        }
    }

    public async Task<KeyInspection> InspectAsync(int keyIndex)
    {
        IDatabase db = redis.GetDatabase();
        string key = RedisKeys.EvictionKey(_opt.KeyPrefix, keyIndex);

        if (!await db.KeyExistsAsync(key))
            return new KeyInspection(key, false, null, null, "evicted or never seeded");

        string policy = (await GetPolicyAsync()).MaxMemoryPolicy;
        if (policy.Contains("lfu"))
        {
            long freq = (long)(await db.ExecuteAsync("OBJECT", "FREQ", key))!;
            return new KeyInspection(key, true, null, freq,
                "FREQ is logarithmic: ~255 needs ~1M hits at lfu-log-factor=10");
        }

        long idle = (long)(await db.ExecuteAsync("OBJECT", "IDLETIME", key))!;
        return new KeyInspection(key, true, idle, null,
            "IDLETIME = seconds since last access (unreliable immediately after an LFU→LRU policy switch)");
    }

    public async Task<EvictionStats> StatsAsync()
    {
        IGrouping<string, KeyValuePair<string, string>>[] info = await Server.InfoAsync();
        var flat = info.SelectMany(g => g).ToDictionary(kv => kv.Key, kv => kv.Value);

        long evicted = long.Parse(flat.GetValueOrDefault("evicted_keys", "0"));
        long expired = long.Parse(flat.GetValueOrDefault("expired_keys", "0"));
        long hits = long.Parse(flat.GetValueOrDefault("keyspace_hits", "0"));
        long misses = long.Parse(flat.GetValueOrDefault("keyspace_misses", "0"));
        long usedMem = long.Parse(flat.GetValueOrDefault("used_memory", "0"));

        return new EvictionStats(
            evicted, expired, hits, misses,
            hits + misses == 0 ? 0 : (double)hits / (hits + misses),
            usedMem);
    }
}
