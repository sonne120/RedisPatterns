using Confluent.Kafka;
using Microsoft.Extensions.Options;
using RedisPatterns.Core.Idempotency;
using RedisPatterns.Core.Projection;

namespace RedisPatterns.Api.Endpoints;

public static class ProjectionEndpoints
{
    public record TransactionRequest(
        string TransactionId, string ClientId, decimal Amount, long Version, int Copies = 1);

    public static IEndpointRouteBuilder MapProjection(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/projection").WithTags("Exactly-once projection");

        group.MapPost("/transactions", async (
            TransactionRequest req, KafkaTransactionProducer producer, CancellationToken ct) =>
        {
            var evt = ToEvent(req);
            var offsets = new List<string>();
            for (int i = 0; i < Math.Clamp(req.Copies, 1, 10); i++)
                offsets.Add(await producer.ProduceAsync(evt, ct));
            return Results.Accepted(value: new
            {
                produced = offsets,
                hint = "Watch the API logs: first delivery → Applied, the rest → DuplicateSkipped*"
            });
        });

        group.MapPost("/apply", async (
            TransactionRequest req, ProjectionHandler handler, CancellationToken ct) =>
        {
            ProjectionOutcome outcome = await handler.ApplyAsync(ToEvent(req), ct);
            return Results.Ok(new { outcome = outcome.ToString() });
        });

        group.MapGet("/clients/{clientId}", async (
            string clientId, ProjectionHandler handler, CancellationToken ct) =>
        {
            var state = await handler.GetClientStateAsync(clientId, ct);
            return state is null ? Results.NotFound() : Results.Ok(state);
        });

        return app;
    }

    private static TransactionEvent ToEvent(TransactionRequest req) =>
        new(req.TransactionId, req.ClientId, req.Amount, req.Version, DateTime.UtcNow);
}

public sealed class KafkaTransactionProducer : IDisposable
{
    private readonly IProducer<string, string>? _producer;
    private readonly KafkaOptions _opt;

    public KafkaTransactionProducer(IOptions<KafkaOptions> options)
    {
        _opt = options.Value;
        if (!_opt.Enabled) return;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = _opt.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();
    }

    public async Task<string> ProduceAsync(TransactionEvent evt, CancellationToken ct)
    {
        if (_producer is null)
            throw new InvalidOperationException("Kafka is disabled (Kafka:Enabled=false). Use POST /projection/apply instead.");
        var result = await _producer.ProduceAsync(
            _opt.TransactionsTopic,
            new Message<string, string>
            {
                Key = evt.ClientId,
                Value = System.Text.Json.JsonSerializer.Serialize(evt)
            }, ct);
        return $"{result.TopicPartition}@{result.Offset.Value}";
    }

    public void Dispose() => _producer?.Dispose();
}
