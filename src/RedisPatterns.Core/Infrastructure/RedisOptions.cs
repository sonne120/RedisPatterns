namespace RedisPatterns.Core.Infrastructure;

public sealed class RedisOptions
{
    public const string SectionName = "Redis";

    public string ConnectionString { get; set; } = "localhost:6379,abortConnect=false,allowAdmin=true";

    public string KeyPrefix { get; set; } = "app";
}
