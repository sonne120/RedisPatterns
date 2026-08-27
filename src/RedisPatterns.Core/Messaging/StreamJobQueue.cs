using System.Text.Json;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Messaging;

public sealed record JobEnvelope(string JobId, string Type, string Payload, int Attempt = 1);

public sealed class StreamJobQueue(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options)
{
    public const string ConsumerGroup = "workers";
    private readonly string _stream = RedisKeys.JobStream(options.Value.KeyPrefix);

    public async Task<string> EnqueueAsync(string type, string payload)
    {
        var job = new JobEnvelope(Guid.NewGuid().ToString("N"), type, payload);
        RedisValue id = await redis.GetDatabase().StreamAddAsync(
            _stream,
            [new NameValueEntry("job", JsonSerializer.Serialize(job))],
            maxLength: 100_000, useApproximateMaxLength: true);
        return id.ToString();
    }

    public async Task EnsureGroupAsync()
    {
        try
        {
            await redis.GetDatabase().StreamCreateConsumerGroupAsync(
                _stream, ConsumerGroup, StreamPosition.NewMessages, createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP")) { }
    }

    public async Task<(long Length, long Pending)> StatsAsync()
    {
        IDatabase db = redis.GetDatabase();
        long length = await db.StreamLengthAsync(_stream);
        StreamPendingInfo pending = await db.StreamPendingAsync(_stream, ConsumerGroup);
        return (length, pending.PendingMessageCount);
    }

    public string StreamKey => _stream;
}
