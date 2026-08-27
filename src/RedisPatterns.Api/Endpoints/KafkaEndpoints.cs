using Confluent.Kafka;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Idempotency;

namespace RedisPatterns.Api.Endpoints;

public sealed class KafkaOrderProducer : IDisposable
{
    private readonly IProducer<string, string>? _producer;
    private readonly KafkaOptions _opt;

    public KafkaOrderProducer(IOptions<KafkaOptions> options)
    {
        _opt = options.Value;
        if (!_opt.Enabled) return;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = _opt.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = 5
        }).Build();
    }

    public async Task<string> ProduceAsync(string eventId, string payload, CancellationToken ct)
    {
        if (_producer is null)
            throw new InvalidOperationException("Kafka is disabled (Kafka:Enabled=false).");
        var result = await _producer.ProduceAsync(
            _opt.OrdersTopic, new Message<string, string> { Key = eventId, Value = payload }, ct);
        return $"{result.TopicPartition}@{result.Offset.Value}";
    }

    public void Dispose() => _producer?.Dispose();
}

public static class KafkaEndpoints
{
    public record OrderEventRequest(string EventId, string Payload, int Copies = 1);

    public static IEndpointRouteBuilder MapKafka(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/kafka").WithTags("Kafka + dedup");

        group.MapPost("/orders", async (OrderEventRequest req, KafkaOrderProducer producer, CancellationToken ct) =>
        {
            var offsets = new List<string>();
            for (int i = 0; i < Math.Clamp(req.Copies, 1, 10); i++)
                offsets.Add(await producer.ProduceAsync(req.EventId, req.Payload, ct));
            return Results.Accepted(value: new
            {
                produced = offsets,
                hint = "Watch the API logs: every copy is consumed, only the first is processed."
            });
        });

        return app;
    }
}
