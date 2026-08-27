using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using RedisPatterns.Core.Infrastructure;
using StackExchange.Redis;

namespace RedisPatterns.Core.Projection;

public sealed class ProjectionHandler(
    IConnectionMultiplexer redis,
    NpgsqlDataSource dataSource,
    IOptions<RedisOptions> options,
    ILogger<ProjectionHandler> logger)
{
    private static readonly TimeSpan MarkTtl = TimeSpan.FromDays(7);

    private readonly RedisOptions _opt = options.Value;

    public async Task<ProjectionOutcome> ApplyAsync(TransactionEvent evt, CancellationToken ct = default)
    {
        string markKey = RedisKeys.ProjectionTxMark(_opt.KeyPrefix, evt.TransactionId);
        IDatabase db = redis.GetDatabase();

        try
        {
            if (await db.KeyExistsAsync(markKey))
                return ProjectionOutcome.DuplicateSkippedByRedis;
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "Redis fast-path unavailable; falling through to Postgres guards");
        }

        bool inserted;
        bool versionAdvanced = false;

        await using (var conn = await dataSource.OpenConnectionAsync(ct))
        await using (var tx = await conn.BeginTransactionAsync(ct))
        {
            int rows = await conn.ExecuteAsync(new CommandDefinition("""
                INSERT INTO transactions_ledger (transaction_id, client_id, amount, version, occurred_at)
                VALUES (@TransactionId, @ClientId, @Amount, @Version, @OccurredAtUtc)
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                new
                {
                    evt.TransactionId,
                    evt.ClientId,
                    evt.Amount,
                    evt.Version,
                    OccurredAtUtc = DateTime.SpecifyKind(evt.OccurredAtUtc, DateTimeKind.Utc)
                },
                transaction: tx, cancellationToken: ct));
            inserted = rows == 1;

            if (inserted)
            {
                versionAdvanced = await conn.ExecuteScalarAsync<bool>(new CommandDefinition("""
                    INSERT INTO client_state (client_id, balance, tx_count, last_amount, version, updated_at)
                    VALUES (@ClientId, @Amount, 1, @Amount, @Version, now())
                    ON CONFLICT (client_id) DO UPDATE SET
                        balance     = client_state.balance + EXCLUDED.balance,
                        tx_count    = client_state.tx_count + 1,
                        last_amount = CASE WHEN EXCLUDED.version > client_state.version
                                           THEN EXCLUDED.last_amount
                                           ELSE client_state.last_amount END,
                        version     = GREATEST(client_state.version, EXCLUDED.version),
                        updated_at  = now()
                    RETURNING version = @Version
                    """,
                    new { evt.ClientId, evt.Amount, evt.Version },
                    transaction: tx, cancellationToken: ct));
            }

            await tx.CommitAsync(ct);
        }

        try
        {
            await db.StringSetAsync(markKey, 1, MarkTtl);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "Failed to set projection mark {Key}; Postgres guard still protects", markKey);
        }

        if (!inserted)
        {
            logger.LogInformation("Duplicate {TxId} stopped by Postgres ON CONFLICT", evt.TransactionId);
            return ProjectionOutcome.DuplicateSkippedByPostgres;
        }
        if (!versionAdvanced)
        {
            logger.LogInformation(
                "Event {TxId} (v{Version}) applied to aggregates; last-value view kept a newer version",
                evt.TransactionId, evt.Version);
            return ProjectionOutcome.AppliedStaleVersion;
        }
        return ProjectionOutcome.Applied;
    }

    public async Task<ClientStateRow?> GetClientStateAsync(string clientId, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<ClientStateRow>(new CommandDefinition("""
            SELECT client_id  AS clientid,
                   balance,
                   tx_count   AS txcount,
                   last_amount AS lastamount,
                   version,
                   updated_at AS updatedat
              FROM client_state
             WHERE client_id = @clientId
            """,
            new { clientId }, cancellationToken: ct));
    }
}

public sealed record ClientStateRow(
    string ClientId, decimal Balance, long TxCount, decimal LastAmount, long Version, DateTime UpdatedAt);
