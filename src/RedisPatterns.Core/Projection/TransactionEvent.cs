namespace RedisPatterns.Core.Projection;

public sealed record TransactionEvent(
    string TransactionId,
    string ClientId,
    decimal Amount,
    long Version,
    DateTime OccurredAtUtc);

public enum ProjectionOutcome
{
    Applied,
    AppliedStaleVersion,
    DuplicateSkippedByRedis,
    DuplicateSkippedByPostgres
}
