using Dapper;
using Npgsql;

namespace RedisPatterns.Core.Data;

public sealed class PostgresProductStore(NpgsqlDataSource dataSource) : IProductStore
{
    public async Task<Product?> GetAsync(long id, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        return await conn.QuerySingleOrDefaultAsync<Product>(new CommandDefinition(
            "SELECT id, name, price, stock, updated_at AS updatedat FROM products WHERE id = @id",
            new { id }, cancellationToken: ct));
    }

    public async Task UpsertAsync(Product product, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(UpsertSql, ToRow(product), cancellationToken: ct));
    }

    public async Task UpsertBatchAsync(IReadOnlyCollection<Product> products, CancellationToken ct = default)
    {
        if (products.Count == 0) return;

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(
            UpsertSql, products.Select(ToRow), transaction: tx, cancellationToken: ct));
        await tx.CommitAsync(ct);
    }

    public async Task DeleteAsync(long id, CancellationToken ct = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await conn.ExecuteAsync(new CommandDefinition(
            "DELETE FROM products WHERE id = @id", new { id }, cancellationToken: ct));
    }

    private const string UpsertSql = """
        INSERT INTO products (id, name, price, stock, updated_at)
        VALUES (@Id, @Name, @Price, @Stock, @UpdatedAt)
        ON CONFLICT (id) DO UPDATE
           SET name = EXCLUDED.name,
               price = EXCLUDED.price,
               stock = EXCLUDED.stock,
               updated_at = EXCLUDED.updated_at
         WHERE products.updated_at <= EXCLUDED.updated_at
        """; 

    private static object ToRow(Product p) => new
    {
        p.Id,
        p.Name,
        p.Price,
        p.Stock,
        UpdatedAt = DateTime.SpecifyKind(p.UpdatedAt, DateTimeKind.Utc)
    };
}
