using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Leaderboard;

public sealed record LeaderboardEntry(long Rank, string Player, double Score);

public sealed class LeaderboardService(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options)
{
    private readonly RedisOptions _opt = options.Value;

    private string Key(string board) => RedisKeys.Leaderboard(_opt.KeyPrefix, board);

    public Task<double> AddScoreAsync(string board, string player, double points) =>
        redis.GetDatabase().SortedSetIncrementAsync(Key(board), player, points);

    public async Task<IReadOnlyList<LeaderboardEntry>> TopAsync(string board, int count = 10)
    {
        SortedSetEntry[] entries = await redis.GetDatabase()
            .SortedSetRangeByRankWithScoresAsync(Key(board), 0, count - 1, Order.Descending);
        return entries
            .Select((e, i) => new LeaderboardEntry(i + 1, e.Element.ToString(), e.Score))
            .ToArray();
    }

    public async Task<(long? Rank, double? Score, double? Percentile)> RankAsync(string board, string player)
    {
        IDatabase db = redis.GetDatabase();
        ITransaction tx = db.CreateTransaction();
        Task<long?> rankT = tx.SortedSetRankAsync(Key(board), player, Order.Descending);
        Task<double?> scoreT = tx.SortedSetScoreAsync(Key(board), player);
        Task<long> totalT = tx.SortedSetLengthAsync(Key(board));
        await tx.ExecuteAsync();

        long? rank = await rankT;
        long total = await totalT;
        if (rank is null || total == 0) return (null, null, null);
        return (rank + 1, await scoreT, 100.0 * (total - rank.Value) / total);
    }

    public async Task<IReadOnlyList<LeaderboardEntry>> AroundMeAsync(string board, string player, int radius = 2)
    {
        IDatabase db = redis.GetDatabase();
        long? rank = await db.SortedSetRankAsync(Key(board), player, Order.Descending);
        if (rank is null) return [];

        long from = Math.Max(0, rank.Value - radius);
        SortedSetEntry[] entries = await db.SortedSetRangeByRankWithScoresAsync(
            Key(board), from, rank.Value + radius, Order.Descending);
        return entries
            .Select((e, i) => new LeaderboardEntry(from + i + 1, e.Element.ToString(), e.Score))
            .ToArray();
    }
}
