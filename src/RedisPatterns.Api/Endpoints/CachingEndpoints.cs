using Microsoft.Extensions.Options;
using RedisPatterns.Core.Caching;
using RedisPatterns.Core.Data;
using RedisPatterns.Core.Infrastructure;

namespace RedisPatterns.Api.Endpoints;

public static class CachingEndpoints
{
    public record SaveProductRequest(string Name, decimal Price, int Stock);

    public static IEndpointRouteBuilder MapCaching(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/cache").WithTags("Caching");

        group.MapGet("/products/{id:long}", async (
            long id, CacheAsideCache cache, IProductStore store,
            IOptions<RedisOptions> redisOpt, CancellationToken ct) =>
        {
            var product = await cache.GetOrLoadAsync(
                RedisKeys.ProductCache(redisOpt.Value.KeyPrefix, id),
                loadCt => store.GetAsync(id, loadCt),
                ct);
            return product is null ? Results.NotFound() : Results.Ok(product);
        });

        group.MapPut("/products/{id:long}", async (
            long id, SaveProductRequest req, CacheAsideCache cache, IProductStore store,
            IOptions<RedisOptions> redisOpt, CancellationToken ct) =>
        {
            await store.UpsertAsync(new Product
            {
                Id = id, Name = req.Name, Price = req.Price, Stock = req.Stock,
                UpdatedAt = DateTime.UtcNow
            }, ct);
            await cache.InvalidateAsync(RedisKeys.ProductCache(redisOpt.Value.KeyPrefix, id));
            return Results.NoContent();
        });

        group.MapPut("/write-through/products/{id:long}", async (
            long id, SaveProductRequest req, WriteThroughProductService svc, CancellationToken ct) =>
        {
            var saved = await svc.SaveAsync(new Product
            {
                Id = id, Name = req.Name, Price = req.Price, Stock = req.Stock
            }, ct);
            return Results.Ok(saved);
        });

        group.MapPut("/write-behind/products/{id:long}", async (
            long id, SaveProductRequest req, WriteBehindProductService svc, CancellationToken ct) =>
        {
            await svc.SaveAsync(new Product
            {
                Id = id, Name = req.Name, Price = req.Price, Stock = req.Stock
            }, ct);
            return Results.Accepted();
        });

        group.MapGet("/write-behind/products/{id:long}", async (
            long id, WriteBehindProductService svc, IProductStore store, CancellationToken ct) =>
        {
            var product = await svc.GetAsync(id, store, ct);
            return product is null ? Results.NotFound() : Results.Ok(product);
        });

        group.MapGet("/hot/products/{id:long}", async (
            long id, TwoTierHotKeyCache cache, IProductStore store,
            IOptions<RedisOptions> redisOpt, CancellationToken ct) =>
        {
            var product = await cache.GetAsync(
                RedisKeys.ProductCache(redisOpt.Value.KeyPrefix, id) + ":hot",
                loadCt => store.GetAsync(id, loadCt),
                ct);
            return product is null ? Results.NotFound() : Results.Ok(product);
        });

        return app;
    }
}
