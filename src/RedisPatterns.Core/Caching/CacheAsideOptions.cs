namespace RedisPatterns.Core.Caching;

public sealed class CacheAsideOptions
{
    public const string SectionName = "CacheAside";

    public TimeSpan Ttl { get; set; } = TimeSpan.FromMinutes(10);

    public TimeSpan TtlJitter { get; set; } = TimeSpan.FromMinutes(2);

    public TimeSpan NegativeTtl { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan RebuildLockTtl { get; set; } = TimeSpan.FromSeconds(10);

    public TimeSpan RebuildWaitTimeout { get; set; } = TimeSpan.FromSeconds(3);
}
