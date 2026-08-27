namespace RedisPatterns.Core.Data;

public interface IProductStore
{
    Task<Product?> GetAsync(long id, CancellationToken ct = default);
    Task UpsertAsync(Product product, CancellationToken ct = default);
    Task UpsertBatchAsync(IReadOnlyCollection<Product> products, CancellationToken ct = default);
    Task DeleteAsync(long id, CancellationToken ct = default);
}
