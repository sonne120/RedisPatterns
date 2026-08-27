using RedisPatterns.Core.Counters;
using RedisPatterns.Core.FeatureFlags;
using RedisPatterns.Core.Geo;
using RedisPatterns.Core.Leaderboard;
using RedisPatterns.Core.Probabilistic;
using RedisPatterns.Core.Sessions;

namespace RedisPatterns.Api.Endpoints;

public static class DataEndpoints
{
    public record LoginRequest(string UserId);
    public record ScoreRequest(string Player, double Points);
    public record FlagRequest(string Value);
    public record PositionRequest(double Lat, double Lon);
    public record ReserveRequest(long Quantity);

    public static IEndpointRouteBuilder MapData(this IEndpointRouteBuilder app)
    {
        var sessions = app.MapGroup("/sessions").WithTags("Session store");

        sessions.MapPost("/login", async (LoginRequest req, RedisSessionStore store) =>
        {
            string sessionId = await store.CreateAsync(req.UserId, new Dictionary<string, string>
            {
                ["role"] = "user"
            });
            return Results.Ok(new { sessionId });
        });

        sessions.MapGet("/{sessionId}", async (string sessionId, RedisSessionStore store) =>
        {
            var session = await store.GetAsync(sessionId);
            return session is null ? Results.NotFound() : Results.Ok(session);
        });

        sessions.MapDelete("/{sessionId}", async (string sessionId, RedisSessionStore store) =>
            await store.DestroyAsync(sessionId) ? Results.NoContent() : Results.NotFound());

        var lb = app.MapGroup("/leaderboard/{board}").WithTags("Leaderboard");

        lb.MapPost("/scores", async (string board, ScoreRequest req, LeaderboardService svc) =>
            Results.Ok(new { newScore = await svc.AddScoreAsync(board, req.Player, req.Points) }));

        lb.MapGet("/top", async (string board, LeaderboardService svc, int count = 10) =>
            Results.Ok(await svc.TopAsync(board, count)));

        lb.MapGet("/players/{player}", async (string board, string player, LeaderboardService svc) =>
        {
            var (rank, score, percentile) = await svc.RankAsync(board, player);
            return rank is null
                ? Results.NotFound()
                : Results.Ok(new { rank, score, percentile, around = await svc.AroundMeAsync(board, player) });
        });

        var counters = app.MapGroup("/counters").WithTags("Counters");

        counters.MapPost("/posts/{postId:long}/like", async (long postId, CounterService svc) =>
            Results.Ok(new { likes = await svc.LikeAsync(postId) }));

        counters.MapPost("/posts/{postId:long}/view", async (long postId, CounterService svc) =>
            Results.Ok(new { views = await svc.TrackViewAsync(postId) }));

        counters.MapPost("/visitors/{userId}", async (string userId, CounterService svc) =>
        {
            await svc.TrackVisitorAsync(userId);
            await svc.HeartbeatAsync(userId);
            return Results.Ok(new
            {
                uniqueToday = await svc.UniqueVisitorsTodayAsync(),
                onlineNow = await svc.OnlineNowAsync()
            });
        });

        counters.MapPost("/inventory/{productId:long}/reserve", async (
            long productId, ReserveRequest req, CounterService svc) =>
        {
            long? remaining = await svc.TryReserveAsync(productId, req.Quantity);
            return remaining is null
                ? Results.Conflict(new { message = "Insufficient stock" })
                : Results.Ok(new { remaining });
        });

        counters.MapPut("/inventory/{productId:long}", async (
            long productId, ReserveRequest req, CounterService svc) =>
        {
            await svc.SetStockAsync(productId, req.Quantity);
            return Results.NoContent();
        });

        var bloom = app.MapGroup("/bloom").WithTags("Bloom filter");

        bloom.MapPost("/events/{eventId}", async (string eventId, BloomFilterService svc) =>
            Results.Ok(new { wasNew = await svc.AddAsync(eventId) }));

        bloom.MapGet("/events/{eventId}", async (string eventId, BloomFilterService svc) =>
            Results.Ok(new
            {
                mightContain = await svc.MightContainAsync(eventId),
                note = "false ⇒ DEFINITELY never seen; true ⇒ probably seen (≈1% false positives)",
                backend = await svc.BackendAsync()
            }));

        var flags = app.MapGroup("/flags").WithTags("Feature flags");

        flags.MapGet("/", (FeatureFlagService svc) => Results.Ok(svc.All()));
        flags.MapGet("/{flag}", (string flag, FeatureFlagService svc) =>
            Results.Ok(new { flag, enabled = svc.IsEnabled(flag), raw = svc.GetValue(flag) }));
        flags.MapPut("/{flag}", async (string flag, FlagRequest req, FeatureFlagService svc) =>
        {
            await svc.SetAsync(flag, req.Value);
            return Results.NoContent();
        });

        var geo = app.MapGroup("/geo/couriers").WithTags("Geospatial");

        geo.MapPut("/{courierId}", async (string courierId, PositionRequest req, CourierGeoService svc) =>
        {
            await svc.UpdatePositionAsync(courierId, req.Lat, req.Lon);
            return Results.NoContent();
        });

        geo.MapGet("/nearby", async (double lat, double lon, CourierGeoService svc, double radiusKm = 5) =>
            Results.Ok(await svc.FindNearbyAsync(lat, lon, radiusKm)));

        geo.MapPost("/sweep", async (CourierGeoService svc) =>
            Results.Ok(new { removedStale = await svc.SweepStaleAsync(TimeSpan.FromMinutes(5)) }));

        return app;
    }
}
