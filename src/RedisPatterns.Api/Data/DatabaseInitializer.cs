using Dapper;
using Npgsql;

namespace RedisPatterns.Api.Data;

public sealed class DatabaseInitializer(
    NpgsqlDataSource dataSource,
    ILogger<DatabaseInitializer> logger) : IHostedService
{
    private const string Schema = """
        CREATE TABLE IF NOT EXISTS products (
            id          BIGINT PRIMARY KEY,
            name        TEXT NOT NULL,
            price       NUMERIC(12,2) NOT NULL,
            stock       INT NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL
        );

        INSERT INTO products (id, name, price, stock, updated_at)
        VALUES
            (1, 'Mechanical keyboard', 129.90, 42, now()),
            (2, 'Ultrawide monitor',   549.00, 7,  now()),
            (3, 'USB-C dock',          89.50,  120, now())
        ON CONFLICT (id) DO NOTHING;

        -- Exactly-once projection (read model). The PRIMARY KEY on
        -- transaction_id IS the permanent dedup guard the ON CONFLICT relies on.
        CREATE TABLE IF NOT EXISTS transactions_ledger (
            transaction_id  TEXT PRIMARY KEY,
            client_id       TEXT NOT NULL,
            amount          NUMERIC(14,2) NOT NULL,
            version         BIGINT NOT NULL,
            occurred_at     TIMESTAMPTZ NOT NULL,
            applied_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS client_state (
            client_id    TEXT PRIMARY KEY,
            balance      NUMERIC(14,2) NOT NULL,   -- commutative: dedup-guarded only
            tx_count     BIGINT NOT NULL,          -- commutative: dedup-guarded only
            last_amount  NUMERIC(14,2) NOT NULL,   -- last-value: version-guarded
            version      BIGINT NOT NULL,          -- max version seen (ordering guard)
            updated_at   TIMESTAMPTZ NOT NULL
        );
        """;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
                await conn.ExecuteAsync(Schema);
                logger.LogInformation("Database schema ready");
                return;
            }
            catch (Exception ex) when (attempt < 10)
            {
                logger.LogWarning("Postgres not ready (attempt {Attempt}): {Message}", attempt, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
