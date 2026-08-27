using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Probabilistic;

public sealed class BloomFilterService
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<BloomFilterService> _logger;
    private readonly string _key;

    private const long ExpectedItems = 1_000_000;
    private const double FalsePositiveRate = 0.01;
    private readonly long _bits;
    private readonly int _hashes;

    private bool? _moduleAvailable;

    public BloomFilterService(
        IConnectionMultiplexer redis,
        IOptions<RedisOptions> options,
        ILogger<BloomFilterService> logger)
    {
        _redis = redis;
        _logger = logger;
        _key = RedisKeys.SeenEventsBloom(options.Value.KeyPrefix);

        _bits = (long)Math.Ceiling(-ExpectedItems * Math.Log(FalsePositiveRate) / (Math.Log(2) * Math.Log(2)));
        _hashes = (int)Math.Max(1, Math.Round(_bits / (double)ExpectedItems * Math.Log(2)));
    }

    public async Task<bool> AddAsync(string item)
    {
        IDatabase db = _redis.GetDatabase();
        if (await ModuleAvailableAsync(db))
        {
            var r = await db.ExecuteAsync("BF.ADD", _key, item);
            return (long)r == 1;
        }
        return await BitmapAddAsync(db, item);
    }

    public async Task<bool> MightContainAsync(string item)
    {
        IDatabase db = _redis.GetDatabase();
        if (await ModuleAvailableAsync(db))
        {
            var r = await db.ExecuteAsync("BF.EXISTS", _key, item);
            return (long)r == 1;
        }
        return await BitmapContainsAsync(db, item);
    }

    public async Task<string> BackendAsync() =>
        await ModuleAvailableAsync(_redis.GetDatabase()) ? "RedisBloom (BF.*)" : $"bitmap fallback (m={_bits} bits, k={_hashes})";

    private async Task<bool> ModuleAvailableAsync(IDatabase db)
    {
        if (_moduleAvailable is { } cached) return cached;
        try
        {
            await db.ExecuteAsync("BF.RESERVE", _key, FalsePositiveRate.ToString("0.####"), ExpectedItems);
            _moduleAvailable = true;
        }
        catch (RedisServerException ex) when (ex.Message.Contains("exists", StringComparison.OrdinalIgnoreCase))
        {
            _moduleAvailable = true;
        }
        catch (RedisServerException)
        {
            _moduleAvailable = false;
            _logger.LogInformation("RedisBloom module not available; using bitmap Bloom fallback");
        }
        return _moduleAvailable.Value;
    }

    private long[] Positions(string item)
    {
        byte[] digest = SHA256.HashData(Encoding.UTF8.GetBytes(item));
        long h1 = BitConverter.ToInt64(digest, 0);
        long h2 = BitConverter.ToInt64(digest, 8) | 1;
        var positions = new long[_hashes];
        for (int i = 0; i < _hashes; i++)
        {
            long combined = unchecked(h1 + i * h2);
            positions[i] = (combined & long.MaxValue) % _bits;
        }
        return positions;
    }

    private const string BitmapAddScript = """
        -- returns 1 if at least one bit was previously 0 (item is new)
        local new = 0
        for i = 1, #ARGV do
            if redis.call('SETBIT', KEYS[1], ARGV[i], 1) == 0 then new = 1 end
        end
        return new
        """;

    private const string BitmapCheckScript = """
        for i = 1, #ARGV do
            if redis.call('GETBIT', KEYS[1], ARGV[i]) == 0 then return 0 end
        end
        return 1
        """;

    private async Task<bool> BitmapAddAsync(IDatabase db, string item)
    {
        RedisValue[] argv = Positions(item).Select(p => (RedisValue)p).ToArray();
        return (long)(await db.ScriptEvaluateAsync(BitmapAddScript, [(RedisKey)_key], argv))! == 1;
    }

    private async Task<bool> BitmapContainsAsync(IDatabase db, string item)
    {
        RedisValue[] argv = Positions(item).Select(p => (RedisValue)p).ToArray();
        return (long)(await db.ScriptEvaluateAsync(BitmapCheckScript, [(RedisKey)_key], argv))! == 1;
    }
}
