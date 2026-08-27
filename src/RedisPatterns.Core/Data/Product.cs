namespace RedisPatterns.Core.Data;

public sealed record Product
{
    public long Id { get; init; }
    public required string Name { get; init; }
    public decimal Price { get; init; }
    public int Stock { get; init; }
    public DateTime UpdatedAt { get; init; }
}
