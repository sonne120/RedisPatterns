using System.Text.Json;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Data;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Caching;

public sealed class WriteBehindProductService(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options)
{
    private readonly RedisOptions _opt = options.Value;

    public async Task SaveAsync(Product product, CancellationToken ct = default)
    {
        product = product with { UpdatedAt = DateTime.UtcNow };
        IDatabase db = redis.GetDatabase();

        string cacheKey = RedisKeys.ProductCache(_opt.KeyPrefix, product.Id);
        string json = JsonSerializer.Serialize(product);

        ITransaction tx = db.CreateTransaction();
        _ = tx.StringSetAsync(cacheKey, json, TimeSpan.FromHours(1));
        _ = tx.StreamAddAsync(
            RedisKeys.WriteBehindStream(_opt.KeyPrefix),
            [new NameValueEntry("id", product.Id), new NameValueEntry("json", json)],
            maxLength: 1_000_000, useApproximateMaxLength: true);

        bool committed = await tx.ExecuteAsync();
        if (!committed)
            throw new InvalidOperationException("Write-behind transaction was not committed.");
    }

    public async Task<Product?> GetAsync(long id, IProductStore fallback, CancellationToken ct = default)
    {
        var raw = await redis.GetDatabase()
            .StringGetAsync(RedisKeys.ProductCache(_opt.KeyPrefix, id));
        if (!raw.IsNull)
            return JsonSerializer.Deserialize<Product>(raw.ToString());
        return await fallback.GetAsync(id, ct);
    }
}
