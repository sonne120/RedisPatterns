using RedisPatterns.Core.Eviction;

namespace RedisPatterns.Api.Endpoints;

public static class EvictionEndpoints
{
    public record PolicyRequest(string Policy, string? MaxMemory);
    public record SeedRequest(int Count, int StartAt = 0);
    public record HeatRequest(int HotCount, int Rounds = 10);
    public record SimulateRequest(int Capacity = 100, int KeySpace = 1000, int Operations = 20000);

    public static IEndpointRouteBuilder MapEviction(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/eviction").WithTags("Eviction (LRU/LFU)");

        group.MapGet("/policy", async (RedisEvictionLab lab) => Results.Ok(await lab.GetPolicyAsync()));

        group.MapPut("/policy", async (PolicyRequest req, RedisEvictionLab lab) =>
        {
            await lab.SetPolicyAsync(req.Policy, req.MaxMemory);
            return Results.Ok(await lab.GetPolicyAsync());
        });

        group.MapPost("/seed", async (SeedRequest req, RedisEvictionLab lab) =>
        {
            await lab.SeedAsync(req.Count, req.StartAt);
            return Results.Ok(new { seeded = req.Count, from = req.StartAt });
        });

        group.MapPost("/heat", async (HeatRequest req, RedisEvictionLab lab) =>
        {
            await lab.HeatAsync(req.HotCount, req.Rounds);
            return Results.Ok(new { heated = req.HotCount, req.Rounds });
        });

        group.MapGet("/inspect/{keyIndex:int}", async (int keyIndex, RedisEvictionLab lab) =>
            Results.Ok(await lab.InspectAsync(keyIndex)));

        group.MapGet("/stats", async (RedisEvictionLab lab) => Results.Ok(await lab.StatsAsync()));

        group.MapPost("/simulate", (SimulateRequest req) =>
        {
            var lru = new LruCache<int, int>(req.Capacity);
            var lfu = new LfuCache<int, int>(req.Capacity);
            var rng = new Random(42);

            int hotSetSize = Math.Max(1, req.KeySpace / 5);
            for (int i = 0; i < req.Operations; i++)
            {
                int key = rng.NextDouble() < 0.8
                    ? rng.Next(hotSetSize)
                    : rng.Next(req.KeySpace);

                if (!lru.TryGet(key, out _)) lru.Put(key, key);
                if (!lfu.TryGet(key, out _)) lfu.Put(key, key);
            }

            for (int i = 0; i < req.Capacity; i++)
            {
                int key = req.KeySpace + i;
                if (!lru.TryGet(key, out _)) lru.Put(key, key);
                if (!lfu.TryGet(key, out _)) lfu.Put(key, key);
            }

            int lruHotSurvivors = Enumerable.Range(0, hotSetSize).Count(k => lru.TryGet(k, out _));
            int lfuHotSurvivors = Enumerable.Range(0, hotSetSize).Count(k => lfu.TryGet(k, out _));

            return Results.Ok(new
            {
                workload = new { req.Capacity, req.KeySpace, req.Operations, hotSetSize, scanBurst = req.Capacity },
                lru = new { lru.Hits, lru.Misses, lru.Evictions, hotKeysStillCached = lruHotSurvivors },
                lfu = new { lfu.Hits, lfu.Misses, lfu.Evictions, hotKeysStillCached = lfuHotSurvivors },
                lfuTopKeys = lfu.Snapshot().Take(10).Select(t => new { key = t.Key, frequency = t.Frequency }),
                conclusion = "After the scan burst, LFU retains far more of the hot set than LRU — " +
                             "frequency resists one-off floods that recency cannot."
            });
        });

        return app;
    }
}
