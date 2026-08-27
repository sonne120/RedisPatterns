namespace RedisPatterns.Core.Infrastructure;

public static class RedisKeys
{
    public static string ProductCache(string prefix, long id) => $"{prefix}:cache:product:{id}";
    public static string CacheLock(string prefix, string cacheKey) => $"{cacheKey}:lock";
    public static string WriteBehindStream(string prefix) => $"{prefix}:wb:products";

    public static string Session(string prefix, string sessionId) => $"{prefix}:session:{sessionId}";
    public static string JwtDenyList(string prefix, string jti) => $"{prefix}:jwt:deny:{jti}";

    public static string Lock(string prefix, string resource) => $"{prefix}:lock:{resource}";

    public static string RateFixed(string prefix, string subject, long windowStamp) => $"{prefix}:rl:fixed:{{{subject}}}:{windowStamp}";
    public static string RateSliding(string prefix, string subject) => $"{prefix}:rl:sliding:{{{subject}}}";
    public static string RateBucket(string prefix, string subject) => $"{prefix}:rl:bucket:{{{subject}}}";

    public static string Idempotency(string prefix, string operationId) => $"{prefix}:idem:{operationId}";

    public static string JobStream(string prefix) => $"{prefix}:jobs:stream";
    public static string DelayedJobs(string prefix) => $"{prefix}:jobs:delayed";

    public static string Leaderboard(string prefix, string board) => $"{prefix}:lb:{board}";

    public static string PostLikes(string prefix, long postId) => $"{prefix}:cnt:likes:{postId}";
    public static string PostViews(string prefix) => $"{prefix}:cnt:views";
    public static string UniqueVisitors(string prefix, DateOnly day) => $"{prefix}:cnt:uv:{day:yyyyMMdd}";
    public static string OnlineUsers(string prefix, long minuteStamp) => $"{prefix}:cnt:online:{minuteStamp}";
    public static string Inventory(string prefix, long productId) => $"{prefix}:inv:{productId}";

    public static string SeenEventsBloom(string prefix) => $"{prefix}:bloom:seen-events";

    public static string ProjectionTxMark(string prefix, string transactionId) => $"{prefix}:rm:tx:{transactionId}";

    public static string FeatureFlags(string prefix) => $"{prefix}:flags";

    public static string Couriers(string prefix) => $"{prefix}:geo:couriers";

    public static string InvalidationChannel(string prefix) => $"{prefix}:ch:invalidate";
    public static string FlagsChangedChannel(string prefix) => $"{prefix}:ch:flags-changed";
    public static string NotificationsChannel(string prefix, string userId) => $"{prefix}:ch:notify:{userId}";

    public static string EvictionKey(string prefix, int i) => $"{prefix}:evict:key:{i}";
}
