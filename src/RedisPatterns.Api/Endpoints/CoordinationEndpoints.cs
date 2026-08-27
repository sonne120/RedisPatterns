using RedisPatterns.Core.Idempotency;
using RedisPatterns.Core.Locking;
using RedisPatterns.Core.RateLimiting;

namespace RedisPatterns.Api.Endpoints;

public static class CoordinationEndpoints
{
    public static IEndpointRouteBuilder MapCoordination(this IEndpointRouteBuilder app)
    {
        var locks = app.MapGroup("/locks").WithTags("Distributed lock");

        locks.MapPost("/report", async (DistributedLockFactory lockFactory, ILoggerFactory lf) =>
        {
            await using DistributedLockHandle? handle =
                await lockFactory.TryAcquireAsync("daily-report", ttl: TimeSpan.FromSeconds(30));
            if (handle is null)
                return Results.Conflict(new { message = "Report is already being generated elsewhere" });

            await Task.Delay(5_000);

            return handle.IsHeld
                ? Results.Ok(new { message = "Report generated", lockWasHeldThroughout = true })
                : Results.Problem("Lock was lost during processing; results were discarded");
        });

        var rates = app.MapGroup("/ratelimit").WithTags("Rate limiting");

        rates.MapGet("/fixed", (HttpContext ctx, FixedWindowRateLimiter limiter) =>
            CheckAsync(ctx, limiter));
        rates.MapGet("/sliding", (HttpContext ctx, SlidingWindowRateLimiter limiter) =>
            CheckAsync(ctx, limiter));
        rates.MapGet("/bucket", (HttpContext ctx, TokenBucketRateLimiter limiter) =>
            CheckAsync(ctx, limiter));

        var idem = app.MapGroup("/payments").WithTags("Idempotency");

        idem.MapPost("/", async (HttpContext ctx, IdempotencyGuard guard) =>
        {
            string? key = ctx.Request.Headers["Idempotency-Key"];
            if (string.IsNullOrWhiteSpace(key))
                return Results.BadRequest(new { message = "Idempotency-Key header is required" });

            var check = await guard.TryBeginAsync($"payment:{key}");
            switch (check.Decision)
            {
                case IdempotencyDecision.AlreadyDone:
                    return Results.Ok(new { replay = true, result = check.StoredResult });

                case IdempotencyDecision.InProgress:
                    return Results.StatusCode(StatusCodes.Status409Conflict);

                default:
                    try
                    {
                        string paymentId = Guid.NewGuid().ToString("N");
                        await guard.CompleteAsync($"payment:{key}", paymentId);
                        return Results.Ok(new { replay = false, result = paymentId });
                    }
                    catch
                    {
                        await guard.FailAsync($"payment:{key}");
                        throw;
                    }
            }
        });

        return app;
    }

    private static async Task<IResult> CheckAsync(HttpContext ctx, IRateLimiter limiter)
    {
        string subject = ctx.User.Identity?.Name
            ?? ctx.Connection.RemoteIpAddress?.ToString()
            ?? "anonymous";

        RateLimitResult result = await limiter.AcquireAsync(subject);
        ctx.Response.Headers["RateLimit-Remaining"] = result.Remaining.ToString();
        if (result.Allowed)
            return Results.Ok(new { allowed = true, result.Remaining });

        ctx.Response.Headers.RetryAfter =
            Math.Max(1, (int)Math.Ceiling(result.RetryAfter.TotalSeconds)).ToString();
        return Results.StatusCode(StatusCodes.Status429TooManyRequests);
    }
}
