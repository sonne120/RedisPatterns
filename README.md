![.NET](https://img.shields.io/badge/.NET-8-512BD4)
![Stars](https://img.shields.io/github/stars/sonne120/RedisPatterns)
![Last commit](https://img.shields.io/github/last-commit/sonne120/RedisPatterns)

[![Downloads](https://img.shields.io/github/downloads/sonne120/RedisPatterns/total)](https://github.com/sonne120/RedisPatterns/releases)
[![Latest](https://img.shields.io/github/downloads/sonne120/RedisPatterns/latest/total)](https://github.com/sonne120/RedisPatterns/releases/latest)


# Redis Patterns

Production-ready Redis patterns built.


| # | Pattern | Implementation | Endpoints |
|---|---------|----------------|-----------|
| 1 | Cache-aside (lazy loading) | `Caching/CacheAsideCache.cs` | `GET /cache/products/{id}` |
| 2 | Write-through | `Caching/WriteThroughProductService.cs` | `PUT /cache/write-through/products/{id}` |
| 3 | Write-behind (write-back) | `Caching/WriteBehindProductService.cs`, `WriteBehindFlusher.cs` | `PUT/GET /cache/write-behind/products/{id}` |
| 4 | Session store + JWT deny-list | `Sessions/RedisSessionStore.cs` | `POST /sessions/login`, `GET/DELETE /sessions/{id}` |
| 5 | Distributed lock | `Locking/DistributedLock.cs` | `POST /locks/report` |
| 6 | Rate limiting ×3 (fixed, sliding, token bucket) | `RateLimiting/*.cs` | `GET /ratelimit/{fixed\|sliding\|bucket}` |
| 7 | Idempotency / dedup (HTTP + Kafka) | `Idempotency/IdempotencyGuard.cs`, `OrderEventsConsumer.cs` | `POST /payments`, `POST /kafka/orders` |
| 8 | Pub/Sub fan-out | `Messaging/NotificationBus.cs` | `POST /notifications/{user}`, SSE `GET /notifications/{user}/stream` |
| 9 | Job queue + delayed jobs (Streams) | `Messaging/StreamJobQueue.cs`, `StreamJobWorker.cs`, `DelayedJobScheduler.cs` | `POST /jobs`, `POST /jobs/delayed`, `GET /jobs/stats` |
| 10 | Leaderboard / ranking | `Leaderboard/LeaderboardService.cs` | `POST /leaderboard/{b}/scores`, `GET /leaderboard/{b}/top` |
| 11 | Counters, HLL, online users, inventory | `Counters/CounterService.cs` | `POST /counters/...` |
| 12 | Bloom filter | `Probabilistic/BloomFilterService.cs` | `POST/GET /bloom/events/{id}` |
| 13 | Hot-key / two-tier cache | `Caching/TwoTierHotKeyCache.cs` | `GET /cache/hot/products/{id}` |
| 14 | Feature flags / config cache | `FeatureFlags/FeatureFlagService.cs` | `GET/PUT /flags/{flag}` |
| 15 | Geospatial | `Geo/CourierGeoService.cs` | `PUT /geo/couriers/{id}`, `GET /geo/couriers/nearby` |
| 16 | **LRU & LFU deep dive** | `Eviction/LruCache.cs`, `LfuCache.cs`, `RedisEvictionLab.cs` | `/eviction/*` |
| 17 | **Exactly-once projection** (at-least-once in, exactly-once projected) | `Projection/ProjectionHandler.cs`, `CdcConsumer.cs` | `POST /projection/{transactions\|apply}`, `GET /projection/clients/{id}` |

## Quick start

Prerequisites: .NET 8 SDK, Docker.

```bash
docker compose up -d                 # Redis Stack, Postgres, Kafka
dotnet run --project src/RedisPatterns.Api
# → http://localhost:5000  (GET / lists every endpoint)
```

Or fully containerized:

```bash
docker compose --profile api up -d --build
# → http://localhost:8080
```

`GET /health` reports Redis (degraded ≠ down: most patterns fail open) and Postgres (unhealthy = down: it is the source of truth).

---

## 1. Cache-aside (lazy loading)

Read from Redis first; on a miss, load from Postgres and cache with a jittered TTL. A `SET NX` per-key lock elects exactly one rebuilder for a hot key — the rest poll the cache briefly, so the dog-pile never reaches the database. "Not found" is cached too (short TTL), and every Redis failure degrades to a direct DB read.

```
         1. GET key                     2. miss → SET NX rebuild-lock
┌─────┐ ────────────────▶ ┌────────┐       (one winner rebuilds,
│ App │ ◀──────────────── │ Redis  │        losers poll the cache)
└─────┘    hit: value     └───▲────┘ ───────────────┐
                              │                     ▼
                              │ 4. SET value   ┌──────────┐
                              │    TTL+jitter  │ Postgres │  3. SELECT
                              └─────────────── └──────────┘
          not found? → cache "NF" sentinel with short TTL (anti-penetration)
```

Use when: read-heavy data that tolerates seconds of staleness — the default cache pattern.

## 2. Write-through

Every write goes to the database first (it is the truth), then to the cache — and if the cache write fails, the key is deleted rather than left stale. Readers see their own writes immediately.

```
┌─────┐ 1. UPSERT (truth first) ┌──────────┐
│ App │ ──────────────────────▶ │ Postgres │
└──┬──┘                         └──────────┘
   │ 2. SET fresh value          ┌───────┐
   └───────────────────────────▶ │ Redis │  cache write failed?
                                 └───────┘  → DEL key (never keep stale)
     readers get read-your-own-write immediately after the update
```

Use when: reads right after writes must be fresh (admin edits a product, storefront reflects it now).

## 3. Write-behind (write-back)

The write is acknowledged as soon as Redis has it; Postgres catches up asynchronously in coalesced batches. One MULTI/EXEC sets the current state **and** appends to a capped Stream — the stream is the replication log a consumer-group flusher drains with XACK-after-commit and XAUTOCLAIM crash recovery.

```
┌─────┐ 1. MULTI/EXEC          ┌──────────────────────────┐   202 Accepted
│ App │ ─────────────────────▶ │ Redis: SET current state │ ─────────▶
└─────┘                        │        XADD wb:products  │
                               └────────────┬─────────────┘
                        XREADGROUP + XAUTOCLAIM (adopt dead consumers)
                                            ▼
                               ┌──────────────────────────┐  batched idempotent
                               │ WriteBehindFlusher       │ ─▶ upsert → Postgres
                               │ coalesce by id, then ACK │  XACK only after commit
                               └──────────────────────────┘
```

Use when: write throughput matters more than a ~1-second durability window (carts, counters, IoT state).

## 4. Session store

One Redis hash per session with a sliding TTL — every read pushes expiry forward, so sessions die of inactivity, work identically on every app instance, and logout is a single DEL. The same TTL mechanics give a free JWT deny-list: a revoked token's `jti` lives exactly as long as the token would have.

```
┌───────┐  Set-Cookie: sid           ┌───────────────────────────┐
│ App A │ ─────────────────────────▶ │ Redis HASH session:{sid}  │
└───────┘                            │  userId · role · ...      │
┌───────┐  every read slides the TTL │  TTL 30m (sliding)        │
│ App B │ ◀────────────────────────▶ └───────────────────────────┘
└───────┘  any instance sees it        logout = DEL → instant revoke
             jwt:deny:{jti} lives exactly as long as the token would
```

Use when: more than one instance runs behind a load balancer (sticky sessions break deploys and autoscaling).

## 5. Distributed lock

`SET key <guid> NX PX`, released by a compare-and-delete Lua script (you can only release *your* lock), renewed by a watchdog so the TTL stays short without expiring under a slow-but-alive holder. `IsHeld` lets long jobs notice a lost lock before committing results.

```
┌──────────┐ SET lock tok1 NX PX ┌───────┐ OK → holder
│ Server 1 │ ──────────────────▶ │       │ ──────────────┐ watchdog renews
└──────────┘                     │ Redis │               ▼ (PEXPIRE @ TTL/3)
┌──────────┐ SET lock tok2 NX PX │       │ nil → busy   release = Lua:
│ Server 2 │ ──────────────────▶ │       │ (409 / wait) "DEL only if
└──────────┘                     └───────┘               value == tok1"
```

Use when: duplicated work is expensive (report generation, cron singletons); when overlap corrupts data, add fencing tokens or use etcd/ZooKeeper — see the class docs and Kleppmann's Redlock analysis.

## 6. Rate limiting ×3

Three algorithms behind one interface, each fully atomic in Lua and returning `Remaining`/`RetryAfter` for standard HTTP headers. The token bucket refills lazily from elapsed time using the *Redis server clock*, so skewed app servers cannot distort it.

```
fixed window            sliding window             token bucket
┌─────────────────┐     ┌─────────────────────┐    ┌─────────────────────┐
│ INCR rl:{s}:{W} │     │ ZREMRANGEBYSCORE    │    │ tokens += Δt · rate │
│  ==1 → PEXPIRE  │     │ ZCARD < limit ?     │    │ (lazy refill, Lua,  │
│  >limit → 429   │     │ ZADD now-rand       │    │  Redis server TIME) │
└─────────────────┘     └─────────────────────┘    └─────────────────────┘
 cheapest, 2× boundary   exact, memory/request      bursts + average rate
 burst possible          (login, SMS, resets)       (public-API style)
```

Use when: fixed for cheap bulk limiting, sliding for exactness on sensitive endpoints, bucket for public APIs that should absorb bursts.

## 7. Idempotency / dedup

A per-operation state machine in one key — `(absent) → "P" (processing, short TTL) → result (long TTL)` — claimed with `SET NX`. The completed entry stores the *original result*, so an HTTP replay answers identically and a Kafka redelivery executes nothing twice. Offsets are committed only after processing.

```
┌───────┐ at-least-once  ┌──────────┐ SET idem:{id} "P" NX  ┌───────┐
│ Kafka │ ─────────────▶ │ Consumer │ ────────────────────▶ │ Redis │
└───▲───┘  (duplicates)  └────┬─────┘                       └───────┘
    │                         │ claimed → process → SET result (24h TTL)
    │                         │ duplicate → return the STORED result
    └── 4. commit offset LAST ┘   (crash mid-work → "P" expires → retry ok)
```

Use when: consuming at-least-once delivery (Kafka, webhooks) or accepting client retries (`Idempotency-Key`); back it with a DB unique constraint when a lost key would double-charge.

## 8. Pub/Sub fan-out

Fire-and-forget by contract: a disconnected subscriber misses the message, full stop. Right for live, ephemeral events — notifications over SSE, cache-invalidation signals — with a bounded drop-oldest buffer so one slow client can't eat unbounded memory.

```
┌───────────┐ PUBLISH ch:notify:{user} ┌───────┐  fan-out  ┌────────────┐
│ Publisher │ ───────────────────────▶ │ Redis │ ────────▶ │ subscriber │─▶ SSE
└───────────┘                          └───┬───┘           └────────────┘
                                           │ offline subscriber: message LOST
                                           ▼ (fire-and-forget by contract)
                              need delivery guarantees? → Streams (pattern 9)
```

Use when: losing a message is acceptable; anything that must be *processed* belongs in Streams or Kafka.

## 9. Job queue + delayed jobs (Streams)

Streams with consumer groups deliver-then-ack (`XREADGROUP`/`XACK`), so a crashed worker's jobs stay pending and are adopted by a live worker via `XAUTOCLAIM`. Bounded retries route poison jobs to a dead-letter stream. Delayed jobs sit in a ZSET scored by due-time; a Lua promoter moves due jobs to the stream atomically, safe to run on every replica.

```
┌──────────┐  XADD           ┌────────────────────┐ XREADGROUP ┌──────────┐
│ Producer │ ──────────────▶ │ Stream jobs:stream │ ─────────▶ │ worker-N │
└──────────┘                 │  (MAXLEN ~100k)    │ ◀── XACK ─ └────┬─────┘
┌──────────┐ ZADD @due-time  └─────────▲──────────┘                 │ fail ×5
│ Delayed  │ ─▶ ZSET ─── promoter ─────┘ (Lua: read+move+del,       ▼
└──────────┘             every 1s        atomic on every replica) ┌───────────┐
   crashed worker? → entries stay pending → XAUTOCLAIM by a live  │ dead-     │
   worker after 30s idle                                          │ letter    │
                                                                  └───────────┘
```

Use when: background work that must not vanish with a crashed worker; lists (`LPUSH`/`BRPOP`) lose the job the moment `BRPOP` returns.

## 10. Leaderboard / ranking

Sorted sets make real-time ranking a primitive: `ZINCRBY` to score, O(log N) rank/top-N/around-me, rank+score+percentile fetched in one transaction. Per-season boards are separate keys — rotation is instant reset with free history.

```
ZINCRBY lb:{board} alice 250
┌───────────────────────────────┐
│ ZSET  #1 alice 9500           │   top-N · rank · percentile · around-me
│       #2 bob   8200           │   all O(log N), millions of members,
│       #3 carol 7100           │   no batch jobs, no ORDER BY scans
└───────────────────────────────┘
```

Use when: ratings, top players, trending products, scoring — anything ranked that updates continuously.

## 11. Counters, HLL, online users, inventory

Four counter shapes, four structures: `INCR` for likes, one `HINCRBY` hash for many per-post counters, HyperLogLog for unique visitors (12 KB, ±0.81%), per-minute HLL buckets merged for "online now" — and inventory as a Lua compare-and-decrement, because a bare `DECR` happily sells below zero.

```
INCR likes:{post}   HINCRBY views {post}   PFADD uv:{day} user
┌─────────────────────────────────────────────────────────────┐
│ HLL: ~uniques in 12KB (±0.81%) · PFMERGE last 5 min buckets │
│      = "online now" with zero cleanup code                  │
│ Lua: stock < qty → -1 (reject) else DECRBY (no oversell)    │
└─────────────────────────────────────────────────────────────┘
```

Use when: high-frequency counting; HLL for dashboards (never billing); the Lua guard whenever a counter has an invariant.

## 12. Bloom filter

Probabilistic membership: *false = definitely never seen, true = probably seen (ε ≈ 1%)*. Two interchangeable backends behind one API — RedisBloom (`BF.*`) when the module is present, otherwise a classic bitmap filter (m/k computed from capacity and ε, Kirsch–Mitzenmacher double hashing, SETBIT/GETBIT batched in Lua). The backend is probed once at startup.

```
             ┌───────────────────────────────┐  0 → DEFINITELY new
 event id ─▶ │ BF.EXISTS  (RedisBloom)       │ ────────────────▶ do the work
             │ or bitmap: k SETBIT/GETBIT    │
             └───────────────┬───────────────┘
                             │ 1 → probably seen (ε ≈ 1%)
                             ▼ skip the expensive DB/API lookup
     sizing: n=1M, ε=1% → m≈9.6M bits (~1.2MB), k=7 · items are never removed
```

Use when: guarding expensive lookups for mostly-absent ids, "already sent/seen?" checks, massive-scale dedup — wherever a rare false positive is acceptable.

## 13. Hot-key / two-tier cache

One hot key saturates one shard (Redis is single-threaded per shard, and a cluster can't split a key). An in-process L1 with a 5-second TTL collapses N requests per instance into ~1 Redis GET per window; L2 reads prefer replicas; writers publish an invalidation that every instance's L1 obeys — with the short L1 TTL as the lossy-pub/sub backstop.

```
┌────────────────┐  L1 miss  ┌────────────────┐  L2 miss  ┌──────────┐
│ L1 in-process  │ ────────▶ │ Redis L2       │ ────────▶ │ Postgres │
│ (5s TTL, free) │           │ PreferReplica  │           └──────────┘
└───────▲────────┘           └───────┬────────┘
        │      writer: SET + PUBLISH │ invalidate
        └────────────────────────────┘ (every instance drops its L1 copy;
                                        short L1 TTL = lossy-pub/sub backstop)
```

Use when: site config, catalog roots, celebrity profiles — the handful of keys that dominate read traffic.

## 14. Feature flags / config cache

All flags live in one hash, snapshotted into process memory: flag reads on the request path cost nothing and keep working from the last snapshot if Redis dies (stale flags beat no flags). Writers publish a change signal for ~1-second propagation; a 30-second timer refresh bounds staleness because pub/sub is lossy.

```
┌───────┐ HSET flags + PUBLISH ┌───────┐  signal   ┌───────────────────────┐
│ Admin │ ───────────────────▶ │ Redis │ ────────▶ │ every instance:       │
└───────┘                      └───────┘           │ HGETALL → local       │
   request path reads the LOCAL snapshot only ◀─── │ snapshot (~1s prop.)  │
   (zero network per request; Redis down →         │ + 30s timer backstop  │
    keep serving last known snapshot)              └───────────────────────┘
```

Use when: kill switches, gradual rollouts, dynamic service config — anything flipped at runtime by people.

## 15. Geospatial

`GEOADD`/`GEOSEARCH` (the modern replacement for deprecated GEORADIUS): live positions upsert by re-adding the member; nearest-first search with distances in one command. Geo keys *are* ZSETs, so the staleness sweep — Redis has no per-member TTL — is a companion last-ping ZSET plus `ZREM`.

```
courier ping ──▶ GEOADD couriers + ZADD last-ping   (one MULTI/EXEC)
                                  │
 GET /nearby ──▶ GEOSEARCH radius ▼ nearest-first · WITHDIST · WITHCOORD
                 ┌───────────────────────────────┐
                 │ ZSET(52-bit geohash) same key │  stale sweep: last-ping
                 └───────────────────────────────┘  < cutoff → ZREM both
```

Use when: "nearest couriers/stores/drivers right now"; graduate to PostGIS for polygons and road distances.

## 16. LRU & LFU eviction

Exact O(1) reference implementations of both algorithms (`LruCache`: dict + doubly-linked list; `LfuCache`: frequency buckets + tracked minimum, with decay) — plus a lab driving the *real* Redis eviction machinery: switch policies at runtime, watch `OBJECT FREQ`/`IDLETIME`, and race both algorithms on one workload. Full deep dive below.

```
LRU — recency list                    LFU — frequency buckets
head(MRU) ⇄ … ⇄ tail(LRU = victim)    f=1 → f=2 → f=5 → … (min tracked)
GET/PUT: unlink, relink at head O(1)  victim = oldest node in min bucket

Redis approximates both: samples `maxmemory-samples` random keys per
eviction + keeps a ~16-entry candidate pool; LFU packs the counter into
8 bits (Morris counter, p = 1/(c·lfu_log_factor+1)) and decays it.
```

Use when: choosing `maxmemory-policy` (see the decision table below) — `allkeys-lfu` for skewed cache workloads, `allkeys-lru` for fast-shifting ones.

## 17. Exactly-once projection

At-least-once in, exactly-once projected — the PacketShard correctness pattern, without Debezium (the write side publishes change events itself). Kafka *will* redeliver; the projection is idempotent instead. `ProjectionHandler` applies a deliberate, crash-safe order; `CdcConsumer` around it is only Kafka plumbing — subscription, the consume loop, and when to commit:

1. **Redis fast-path** — read-only `EXISTS rm:tx:{id}` skips known duplicates before they cost a Postgres round-trip. Redis is a filter, never the source of truth.
2. **Postgres commit** — one transaction, both guards: dedup `INSERT … ON CONFLICT (transaction_id) DO NOTHING` (permanent, not TTL-bound) and the ordering upsert `WHERE EXCLUDED.version > client_state.version` (drops "hello from the past" for last-value views; inert for commutative counts, which dedup alone makes exactly-once).
3. **Redis mark** — `SET rm:tx:{id}` only after the commit.
4. **Kafka ack** — the offset is committed last.

```
┌──────────┐ at-least-once ┌────────────┐ 1. EXISTS rm:tx:{id}   ┌────────┐
│  Kafka   │ ────────────▶ │ Projection │ ─────────────────────▶ │ Redis  │
│ (dups,   │               │  Handler   │ 3. SET mark — only     │ filter,│
│ replays) │               └─────┬──────┘    AFTER the commit    │ not    │
└────▲─────┘                     │                               │ truth  │
     │ 4. commit offset LAST     │ 2. ONE transaction:           └────────┘
     └───────────────────────────│    INSERT … ON CONFLICT (tx_id) DO NOTHING
                                 │    + client_state upsert
                                 ▼      WHERE EXCLUDED.version > version
                         ┌───────────────┐
                         │   Postgres    │  ledger  = permanent dedup (PK)
                         │ (the truth)   │  client_state = ordering guard
                         └───────────────┘
```

Walk every crash point and nothing is lost or doubled: a crash between 2 and 3/4 redelivers into `ON CONFLICT DO NOTHING`; between 3 and 4 the fast-path catches it; a full Redis wipe just rebuilds the filter. Persist first, acknowledge last — inverting any step quietly turns at-least-once into at-most-once.

Use when: building read models / projections from Kafka or CDC streams where double-applying or losing an event is unacceptable.

---

## Try each pattern in 60 seconds

```bash
BASE=http://localhost:5000

# 1. Cache-aside — first call hits Postgres, second is a Redis hit
curl $BASE/cache/products/1
curl $BASE/cache/products/1

# 2/3. Write-through vs write-behind
curl -X PUT $BASE/cache/write-through/products/2 -H 'content-type: application/json' \
     -d '{"name":"Monitor v2","price":499,"stock":5}'          # 200 — DB and cache updated synchronously
curl -X PUT $BASE/cache/write-behind/products/3 -H 'content-type: application/json' \
     -d '{"name":"Dock v2","price":79,"stock":200}'            # 202 — Redis now, Postgres shortly (watch the logs)

# 5. Distributed lock — run these in two terminals simultaneously; one gets 409
curl -X POST $BASE/locks/report

# 6. Rate limiting — hammer it, watch 429 + Retry-After appear
for i in $(seq 1 15); do curl -s -o /dev/null -w "%{http_code} " $BASE/ratelimit/bucket; done

# 7. Idempotency — same key twice: second response is the stored replay
curl -X POST $BASE/payments -H 'Idempotency-Key: abc123'
curl -X POST $BASE/payments -H 'Idempotency-Key: abc123'

# 7b. Kafka dedup — 3 duplicate deliveries, one execution (see API logs)
curl -X POST $BASE/kafka/orders -H 'content-type: application/json' \
     -d '{"eventId":"evt-1","payload":"order 42","copies":3}'

# 8. Pub/Sub — terminal A subscribes, terminal B publishes
curl -N $BASE/notifications/alice/stream          # A
curl -X POST $BASE/notifications/alice -H 'content-type: application/json' \
     -d '{"message":"hello"}'                     # B

# 9. Jobs — a normal job, a poison job (watch retries → dead letter), a delayed job
curl -X POST $BASE/jobs -H 'content-type: application/json' -d '{"type":"email","payload":"to:bob"}'
curl -X POST $BASE/jobs -H 'content-type: application/json' -d '{"type":"fail","payload":"poison"}'
curl -X POST $BASE/jobs/delayed -H 'content-type: application/json' -d '{"type":"email","payload":"later","delaySeconds":10}'
curl $BASE/jobs/stats

# 10. Leaderboard
curl -X POST $BASE/leaderboard/global/scores -H 'content-type: application/json' -d '{"player":"alice","points":9500}'
curl -X POST $BASE/leaderboard/global/scores -H 'content-type: application/json' -d '{"player":"bob","points":8200}'
curl $BASE/leaderboard/global/top
curl $BASE/leaderboard/global/players/alice

# 11. Inventory — the guarded decrement never oversells
curl -X PUT  $BASE/counters/inventory/1 -H 'content-type: application/json' -d '{"quantity":3}'
curl -X POST $BASE/counters/inventory/1/reserve -H 'content-type: application/json' -d '{"quantity":2}'   # ok
curl -X POST $BASE/counters/inventory/1/reserve -H 'content-type: application/json' -d '{"quantity":2}'   # 409

# 12. Bloom filter
curl -X POST $BASE/bloom/events/evt-99
curl $BASE/bloom/events/evt-99          # probably seen
curl $BASE/bloom/events/evt-100         # definitely not

# 14. Feature flags — flipped flag propagates to every instance in ~1s
curl -X PUT $BASE/flags/new-checkout -H 'content-type: application/json' -d '{"value":"on"}'
curl $BASE/flags/new-checkout

# 15. Geo
curl -X PUT $BASE/geo/couriers/c1 -H 'content-type: application/json' -d '{"lat":50.4501,"lon":30.5234}'
curl -X PUT $BASE/geo/couriers/c2 -H 'content-type: application/json' -d '{"lat":50.4547,"lon":30.5238}'
curl "$BASE/geo/couriers/nearby?lat=50.45&lon=30.52&radiusKm=3"

# 17. Exactly-once projection — same event 3× through Kafka: applied once
curl -X POST $BASE/projection/transactions -H 'content-type: application/json' \
     -d '{"transactionId":"tx-1","clientId":"c-7","amount":100,"version":1,"copies":3}'
# or without a broker — the handler returns the outcome directly:
curl -X POST $BASE/projection/apply -H 'content-type: application/json' \
     -d '{"transactionId":"tx-2","clientId":"c-7","amount":50,"version":3}'   # Applied
curl -X POST $BASE/projection/apply -H 'content-type: application/json' \
     -d '{"transactionId":"tx-2","clientId":"c-7","amount":50,"version":3}'   # DuplicateSkippedByRedis
curl -X POST $BASE/projection/apply -H 'content-type: application/json' \
     -d '{"transactionId":"tx-3","clientId":"c-7","amount":25,"version":2}'   # AppliedStaleVersion
curl $BASE/projection/clients/c-7
```

## LRU & LFU — the deep dive (pattern 16)

Three artifacts, meant to be read together:

**`Eviction/LruCache.cs`** — exact LRU in ~100 lines: `Dictionary` + doubly-linked list, every operation O(1); head = most recent, tail = the victim. The file's header explains why Redis *doesn't* do this exactly (per-key pointer overhead + list surgery on every read) and what it does instead: a 24-bit access clock per object, `maxmemory-samples` random keys per eviction (default 5), and a persistent ~16-entry eviction pool — ≈ true LRU quality at near-zero bookkeeping cost.

**`Eviction/LfuCache.cs`** — exact O(1) LFU: frequency buckets + a tracked minimum frequency; within a bucket, ties break by recency. The header maps each piece to the real Redis implementation: the **8-bit Morris counter** incremented with probability `1/(count·lfu_log_factor+1)` (logarithmic — ~1M hits to reach 255 at the default factor 10) and **decay** via `lfu-decay-time` (the class exposes `DecayAll()` so the same idea is testable — without decay, LFU fossilizes around last week's hot keys).

**`Eviction/RedisEvictionLab.cs` + `/eviction/*` endpoints** — drive the real thing:

```bash
# Configure a small memory budget with LFU
curl -X PUT $BASE/eviction/policy -H 'content-type: application/json' \
     -d '{"policy":"allkeys-lfu","maxMemory":"8mb"}'

curl -X POST $BASE/eviction/seed -H 'content-type: application/json' -d '{"count":4000}'
curl -X POST $BASE/eviction/heat -H 'content-type: application/json' -d '{"hotCount":50,"rounds":20}'
curl -X POST $BASE/eviction/seed -H 'content-type: application/json' -d '{"count":4000,"startAt":4000}'   # force evictions

curl $BASE/eviction/inspect/10      # hot key → exists, high OBJECT FREQ
curl $BASE/eviction/inspect/3000    # cold key → likely evicted
curl $BASE/eviction/stats           # evicted_keys, hit rate, used_memory
```

Switch `"policy":"allkeys-lru"` and repeat: `inspect` now returns `OBJECT IDLETIME`, and survival is decided by recency, not frequency. And `POST /eviction/simulate` runs one identical zipf-skewed workload through the reference LRU and LFU side by side, then hits both with a one-off scan burst — the response shows LFU retaining the hot set that LRU flushed, which is precisely the argument for `allkeys-lfu` on real cache workloads.

### Choosing a policy

| Situation | Policy |
|---|---|
| Pure cache, skewed access (the common case) | `allkeys-lfu` (tune `lfu-log-factor`, `lfu-decay-time`) |
| Pure cache, access pattern shifts fast | `allkeys-lru` |
| Cache + persistent keys in one instance | `volatile-lfu`/`volatile-lru` + strict TTL discipline on cache keys (no-TTL keys make volatile-* behave like noeviction!) |
| Expiry encodes usefulness deliberately | `volatile-ttl` |
| Uniform access, no hot set | `allkeys-random` |
| Redis as primary store / queues / streams | `noeviction` + generous `maxmemory` + alert on `used_memory` and any OOM write error |

Watch `evicted_keys` in `INFO stats` (surfaced by `/eviction/stats`): climbing evictions on a cache is normal; on `volatile-*` it can mean TTL discipline slipped; on `noeviction` an OOM error is a page.


