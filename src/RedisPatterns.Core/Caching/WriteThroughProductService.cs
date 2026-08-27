using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Data;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Caching;

public sealed class WriteThroughProductService(
    IConnectionMultiplexer redis,
    IProductStore store,
    IOptions<RedisOptions> redisOptions,
    IOptions<CacheAsideOptions> cacheOptions,
    ILogger<WriteThroughProductService> logger)
{
    private readonly RedisOptions _redisOpt = redisOptions.Value;
    private readonly CacheAsideOptions _cacheOpt = cacheOptions.Value;

    public async Task<Product> SaveAsync(Product product, CancellationToken ct = default)
    {
        product = product with { UpdatedAt = DateTime.UtcNow };

        await store.UpsertAsync(product, ct);

        string key = RedisKeys.ProductCache(_redisOpt.KeyPrefix, product.Id);
        IDatabase db = redis.GetDatabase();
        try
        {
            var ttl = _cacheOpt.Ttl + TimeSpan.FromMilliseconds(
                Random.Shared.NextDouble() * _cacheOpt.TtlJitter.TotalMilliseconds);
            await db.StringSetAsync(key, JsonSerializer.Serialize(product), ttl);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "Write-through cache update failed for {Key}; invalidating instead", key);
            try { await db.KeyDeleteAsync(key, CommandFlags.FireAndForget); }
            catch (RedisException) {  }
        }

        return product;
    }
}
