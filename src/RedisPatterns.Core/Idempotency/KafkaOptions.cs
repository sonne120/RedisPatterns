namespace RedisPatterns.Core.Idempotency;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string OrdersTopic { get; set; } = "orders";
    public string ConsumerGroup { get; set; } = "order-processor";

    public string TransactionsTopic { get; set; } = "transactions";
    public string ProjectionGroup { get; set; } = "srv-read-projection";

    public bool Enabled { get; set; } = true;
}
