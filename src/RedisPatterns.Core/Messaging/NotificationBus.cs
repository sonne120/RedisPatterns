using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Messaging;

public sealed class NotificationBus(
    IConnectionMultiplexer redis,
    IOptions<RedisOptions> options,
    ILogger<NotificationBus> logger)
{
    private readonly RedisOptions _opt = options.Value;

    public async Task<long> PublishAsync(string userId, string message)
    {
        long receivers = await redis.GetSubscriber().PublishAsync(
            RedisChannel.Literal(RedisKeys.NotificationsChannel(_opt.KeyPrefix, userId)), message);
        logger.LogDebug("Notification for {UserId} reached {Receivers} subscriber(s)", userId, receivers);
        return receivers;
    }

    public async IAsyncEnumerable<string> SubscribeAsync(
        string userId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var channel = System.Threading.Channels.Channel.CreateBounded<string>(
            new System.Threading.Channels.BoundedChannelOptions(256)
            {
                FullMode = System.Threading.Channels.BoundedChannelFullMode.DropOldest
            });

        var redisChannel = RedisChannel.Literal(RedisKeys.NotificationsChannel(_opt.KeyPrefix, userId));
        ISubscriber sub = redis.GetSubscriber();

        void Handler(RedisChannel _, RedisValue value) => channel.Writer.TryWrite(value.ToString());
        await sub.SubscribeAsync(redisChannel, Handler);

        try
        {
            await foreach (string msg in channel.Reader.ReadAllAsync(ct))
                yield return msg;
        }
        finally
        {
            await sub.UnsubscribeAsync(redisChannel, Handler);
        }
    }
}
