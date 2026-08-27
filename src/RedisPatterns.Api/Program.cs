using Npgsql;
using RedisPatterns.Api.Data;
using RedisPatterns.Api.Endpoints;
using RedisPatterns.Api.HealthChecks;
using RedisPatterns.Core.Caching;
using RedisPatterns.Core.Counters;
using RedisPatterns.Core.Data;
using RedisPatterns.Core.Eviction;
using RedisPatterns.Core.FeatureFlags;
using RedisPatterns.Core.Geo;
using RedisPatterns.Core.Idempotency;
using RedisPatterns.Core.Infrastructure;
using RedisPatterns.Core.Leaderboard;
using RedisPatterns.Core.Locking;
using RedisPatterns.Core.Messaging;
using RedisPatterns.Core.Probabilistic;
using RedisPatterns.Core.Projection;
using RedisPatterns.Core.RateLimiting;
using RedisPatterns.Core.Sessions;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<RedisOptions>(builder.Configuration.GetSection(RedisOptions.SectionName));
builder.Services.Configure<CacheAsideOptions>(builder.Configuration.GetSection(CacheAsideOptions.SectionName));
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    string cs = builder.Configuration.GetSection(RedisOptions.SectionName)["ConnectionString"]
        ?? "localhost:6379,abortConnect=false,allowAdmin=true";
    return ConnectionMultiplexer.Connect(cs);
});

builder.Services.AddSingleton(_ =>
    NpgsqlDataSource.Create(builder.Configuration.GetConnectionString("Postgres")
        ?? "Host=localhost;Database=patterns;Username=patterns;Password=patterns"));

builder.Services.AddMemoryCache();

builder.Services.AddSingleton<IProductStore, PostgresProductStore>();

builder.Services.AddSingleton<CacheAsideCache>();
builder.Services.AddSingleton<WriteThroughProductService>();
builder.Services.AddSingleton<WriteBehindProductService>();
builder.Services.AddSingleton<TwoTierHotKeyCache>();
builder.Services.AddSingleton<DistributedLockFactory>();
builder.Services.AddSingleton<IdempotencyGuard>();
builder.Services.AddSingleton<NotificationBus>();
builder.Services.AddSingleton<StreamJobQueue>();
builder.Services.AddSingleton<DelayedJobScheduler>();
builder.Services.AddSingleton<RedisSessionStore>();
builder.Services.AddSingleton<LeaderboardService>();
builder.Services.AddSingleton<CounterService>();
builder.Services.AddSingleton<BloomFilterService>();
builder.Services.AddSingleton<FeatureFlagService>();
builder.Services.AddSingleton<CourierGeoService>();
builder.Services.AddSingleton<RedisEvictionLab>();
builder.Services.AddSingleton<KafkaOrderProducer>();
builder.Services.AddSingleton<ProjectionHandler>();
builder.Services.AddSingleton<KafkaTransactionProducer>();

builder.Services.AddSingleton(sp => new FixedWindowRateLimiter(
    sp.GetRequiredService<IConnectionMultiplexer>(),
    sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<RedisOptions>>(),
    limit: 100, window: TimeSpan.FromMinutes(1)));
builder.Services.AddSingleton(sp => new SlidingWindowRateLimiter(
    sp.GetRequiredService<IConnectionMultiplexer>(),
    sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<RedisOptions>>(),
    limit: 20, window: TimeSpan.FromMinutes(1)));
builder.Services.AddSingleton(sp => new TokenBucketRateLimiter(
    sp.GetRequiredService<IConnectionMultiplexer>(),
    sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<RedisOptions>>(),
    capacity: 10, refillRatePerSecond: 2));

builder.Services.AddHostedService<DatabaseInitializer>();
builder.Services.AddHostedService<WriteBehindFlusher>();
builder.Services.AddHostedService<StreamJobWorker>();
builder.Services.AddHostedService<DelayedJobPromoter>();
builder.Services.AddHostedService<OrderEventsConsumer>();
builder.Services.AddHostedService<CdcConsumer>();

builder.Services.AddHealthChecks()
    .AddCheck<RedisHealthCheck>("redis")
    .AddCheck<PostgresHealthCheck>("postgres");

var app = builder.Build();

app.MapGet("/", () => Results.Ok(new
{
    service = "redis-patterns",
    patterns = new[]
    {
        "GET  /cache/products/{id}                  cache-aside (stampede-protected)",
        "PUT  /cache/products/{id}                  write + invalidate",
        "PUT  /cache/write-through/products/{id}    write-through",
        "PUT  /cache/write-behind/products/{id}     write-behind (202, async flush)",
        "GET  /cache/hot/products/{id}              two-tier hot-key cache",
        "POST /locks/report                         distributed lock",
        "GET  /ratelimit/{fixed|sliding|bucket}     rate limiting x3",
        "POST /payments  (Idempotency-Key header)   HTTP idempotency",
        "POST /kafka/orders                         Kafka produce + consumer dedup",
        "POST /jobs | /jobs/delayed | GET /jobs/stats   stream job queue",
        "POST /notifications/{user} | GET /notifications/{user}/stream   pub/sub + SSE",
        "POST /sessions/login | GET/DELETE /sessions/{id}   session store",
        "POST /leaderboard/{b}/scores | GET /leaderboard/{b}/top   sorted sets",
        "POST /counters/...                         counters, HLL, inventory",
        "POST /bloom/events/{id} | GET /bloom/events/{id}   bloom filter",
        "GET/PUT /flags/{flag}                      feature flags",
        "PUT /geo/couriers/{id} | GET /geo/couriers/nearby   geospatial",
        "POST /projection/transactions | /projection/apply | GET /projection/clients/{id}   exactly-once projection",
        "GET/PUT /eviction/policy, POST /eviction/{seed|heat}, GET /eviction/{inspect/{i}|stats}, POST /eviction/simulate   LRU/LFU lab",
        "GET  /health"
    }
}));

app.MapCaching();
app.MapCoordination();
app.MapMessaging();
app.MapData();
app.MapKafka();
app.MapProjection();
app.MapEviction();
app.MapHealthChecks("/health");

app.Run();
