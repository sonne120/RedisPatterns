using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Sessions;

public sealed class RedisSessionStore(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options)
{
    private static readonly TimeSpan SlidingTtl = TimeSpan.FromMinutes(30);
    private readonly RedisOptions _opt = options.Value;

    public async Task<string> CreateAsync(string userId, IDictionary<string, string>? attributes = null)
    {
        string sessionId = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(16));
        string key = RedisKeys.Session(_opt.KeyPrefix, sessionId);

        var entries = new List<HashEntry>
        {
            new("userId", userId),
            new("createdAt", DateTimeOffset.UtcNow.ToUnixTimeSeconds())
        };
        if (attributes is not null)
            entries.AddRange(attributes.Select(kv => new HashEntry(kv.Key, kv.Value)));

        IDatabase db = redis.GetDatabase();
        ITransaction tx = db.CreateTransaction();
        _ = tx.HashSetAsync(key, entries.ToArray());
        _ = tx.KeyExpireAsync(key, SlidingTtl);
        await tx.ExecuteAsync();
        return sessionId;
    }

    public async Task<Dictionary<string, string>?> GetAsync(string sessionId)
    {
        string key = RedisKeys.Session(_opt.KeyPrefix, sessionId);
        IDatabase db = redis.GetDatabase();

        ITransaction tx = db.CreateTransaction();
        Task<HashEntry[]> read = tx.HashGetAllAsync(key);
        _ = tx.KeyExpireAsync(key, SlidingTtl);
        await tx.ExecuteAsync();

        HashEntry[] entries = await read;
        return entries.Length == 0
            ? null
            : entries.ToDictionary(e => e.Name.ToString(), e => e.Value.ToString());
    }

    public Task SetAttributeAsync(string sessionId, string name, string value) =>
        redis.GetDatabase().HashSetAsync(RedisKeys.Session(_opt.KeyPrefix, sessionId), name, value);

    public Task<bool> DestroyAsync(string sessionId) =>
        redis.GetDatabase().KeyDeleteAsync(RedisKeys.Session(_opt.KeyPrefix, sessionId));

    public Task RevokeJwtAsync(string jti, TimeSpan remainingTokenLifetime) =>
        redis.GetDatabase().StringSetAsync(
            RedisKeys.JwtDenyList(_opt.KeyPrefix, jti), "revoked", remainingTokenLifetime);

    public Task<bool> IsJwtRevokedAsync(string jti) =>
        redis.GetDatabase().KeyExistsAsync(RedisKeys.JwtDenyList(_opt.KeyPrefix, jti));
}
