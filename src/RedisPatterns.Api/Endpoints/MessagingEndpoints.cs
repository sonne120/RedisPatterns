using RedisPatterns.Core.Messaging;

namespace RedisPatterns.Api.Endpoints;

public static class MessagingEndpoints
{
    public record EnqueueRequest(string Type, string Payload);
    public record ScheduleRequest(string Type, string Payload, int DelaySeconds);
    public record NotifyRequest(string Message);

    public static IEndpointRouteBuilder MapMessaging(this IEndpointRouteBuilder app)
    {
        var jobs = app.MapGroup("/jobs").WithTags("Job queue");

        jobs.MapPost("/", async (EnqueueRequest req, StreamJobQueue queue) =>
        {
            string id = await queue.EnqueueAsync(req.Type, req.Payload);
            return Results.Accepted(value: new { streamEntryId = id });
        });

        jobs.MapPost("/delayed", async (ScheduleRequest req, DelayedJobScheduler scheduler) =>
        {
            await scheduler.ScheduleAsync(req.Type, req.Payload, TimeSpan.FromSeconds(req.DelaySeconds));
            return Results.Accepted(value: new { dueInSeconds = req.DelaySeconds });
        });

        jobs.MapGet("/stats", async (StreamJobQueue queue, DelayedJobScheduler scheduler) =>
        {
            (long length, long pending) = await queue.StatsAsync();
            long delayed = await scheduler.DueCountAsync();
            return Results.Ok(new { streamLength = length, pendingUnacked = pending, delayedScheduled = delayed });
        });

        var notify = app.MapGroup("/notifications").WithTags("Pub/Sub");

        notify.MapPost("/{userId}", async (string userId, NotifyRequest req, NotificationBus bus) =>
        {
            long receivers = await bus.PublishAsync(userId, req.Message);
            return Results.Ok(new { deliveredToSubscribers = receivers });
        });

        notify.MapGet("/{userId}/stream", async (string userId, NotificationBus bus, HttpContext ctx) =>
        {
            ctx.Response.Headers.ContentType = "text/event-stream";
            ctx.Response.Headers.CacheControl = "no-cache";
            await foreach (string msg in bus.SubscribeAsync(userId, ctx.RequestAborted))
            {
                await ctx.Response.WriteAsync($"data: {msg}\n\n", ctx.RequestAborted);
                await ctx.Response.Body.FlushAsync(ctx.RequestAborted);
            }
        });

        return app;
    }
}
