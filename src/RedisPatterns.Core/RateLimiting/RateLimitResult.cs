namespace RedisPatterns.Core.RateLimiting;

public readonly record struct RateLimitResult(bool Allowed, long Remaining, TimeSpan RetryAfter)
{
    public static RateLimitResult Allow(long remaining) => new(true, remaining, TimeSpan.Zero);
    public static RateLimitResult Block(TimeSpan retryAfter) => new(false, 0, retryAfter);
}

public interface IRateLimiter
{
    Task<RateLimitResult> AcquireAsync(string subject, CancellationToken ct = default);
}
