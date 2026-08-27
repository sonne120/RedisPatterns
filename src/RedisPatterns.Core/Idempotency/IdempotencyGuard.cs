using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Idempotency;

public enum IdempotencyDecision
{
    Execute,
    InProgress,
    AlreadyDone
}

public readonly record struct IdempotencyCheck(IdempotencyDecision Decision, string? StoredResult);

public sealed class IdempotencyGuard(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options)
{
    private const string ProcessingMarker = "P";
    private static readonly TimeSpan ProcessingTtl = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan DoneTtl = TimeSpan.FromHours(24);

    private readonly RedisOptions _opt = options.Value;

    public async Task<IdempotencyCheck> TryBeginAsync(string operationId)
    {
        IDatabase db = redis.GetDatabase();
        string key = RedisKeys.Idempotency(_opt.KeyPrefix, operationId);

        bool claimed = await db.StringSetAsync(
            key, ProcessingMarker, expiry: ProcessingTtl, when: When.NotExists);
        if (claimed)
            return new(IdempotencyDecision.Execute, null);

        var existing = await db.StringGetAsync(key);
        if (existing == ProcessingMarker)
            return new(IdempotencyDecision.InProgress, null);
        if (existing.IsNull)
            return new(IdempotencyDecision.InProgress, null);

        return new(IdempotencyDecision.AlreadyDone, existing.ToString());
    }

    public Task CompleteAsync(string operationId, string result) =>
        redis.GetDatabase().StringSetAsync(
            RedisKeys.Idempotency(_opt.KeyPrefix, operationId), result, DoneTtl);

    public Task FailAsync(string operationId) =>
        redis.GetDatabase().KeyDeleteAsync(
            RedisKeys.Idempotency(_opt.KeyPrefix, operationId));
}
