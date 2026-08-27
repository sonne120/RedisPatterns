using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Geo;

public sealed record NearbyCourier(string CourierId, double DistanceKm, double? Lat, double? Lon);

public sealed class CourierGeoService(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options)
{
    private readonly RedisOptions _opt = options.Value;

    private string GeoKey => RedisKeys.Couriers(_opt.KeyPrefix);
    private string PingKey => GeoKey + ":last-ping";

    public async Task UpdatePositionAsync(string courierId, double lat, double lon)
    {
        IDatabase db = redis.GetDatabase();
        ITransaction tx = db.CreateTransaction();
        _ = tx.GeoAddAsync(GeoKey, new GeoEntry(lon, lat, courierId));
        _ = tx.SortedSetAddAsync(PingKey, courierId, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        await tx.ExecuteAsync();
    }

    public async Task<IReadOnlyList<NearbyCourier>> FindNearbyAsync(
        double lat, double lon, double radiusKm, int count = 10)
    {
        GeoRadiusResult[] results = await redis.GetDatabase().GeoSearchAsync(
            GeoKey, lon, lat,
            new GeoSearchCircle(radiusKm, GeoUnit.Kilometers),
            count: count,
            order: Order.Ascending,
            options: GeoRadiusOptions.WithDistance | GeoRadiusOptions.WithCoordinates);

        return results.Select(r => new NearbyCourier(
            r.Member.ToString(),
            r.Distance ?? 0,
            r.Position?.Latitude,
            r.Position?.Longitude)).ToArray();
    }

    public async Task<double?> DistanceKmAsync(string courierA, string courierB) =>
        await redis.GetDatabase().GeoDistanceAsync(GeoKey, courierA, courierB, GeoUnit.Kilometers);

    public async Task<long> SweepStaleAsync(TimeSpan maxAge)
    {
        IDatabase db = redis.GetDatabase();
        long cutoff = DateTimeOffset.UtcNow.Subtract(maxAge).ToUnixTimeSeconds();

        RedisValue[] stale = await db.SortedSetRangeByScoreAsync(PingKey, double.NegativeInfinity, cutoff);
        if (stale.Length == 0) return 0;

        ITransaction tx = db.CreateTransaction();
        _ = tx.SortedSetRemoveAsync(GeoKey, stale);
        _ = tx.SortedSetRemoveAsync(PingKey, stale);
        await tx.ExecuteAsync();
        return stale.Length;
    }
}
