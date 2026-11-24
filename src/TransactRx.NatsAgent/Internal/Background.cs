using Microsoft.Extensions.Logging;
using NATS.Client.Core;

namespace TransactRx.NatsAgent.Internal;

internal static class Background
{
    /// <summary>
    /// Starts <paramref name="work"/> on the thread pool without the caller's ambient state
    /// (<see cref="CallContext"/>), so a token or user context in scope where a service was
    /// started never leaks into the requests it later serves.
    /// </summary>
    public static Task Run(Func<Task> work)
    {
        using (ExecutionContext.SuppressFlow())
        {
            return Task.Run(work);
        }
    }

    /// <summary>
    /// Reads <paramref name="sub"/> until it ends, handing each message to
    /// <paramref name="handle"/>. A failing handler is logged and the loop goes on.
    /// </summary>
    public static Task Pump(INatsSub<byte[]> sub, ILogger logger, Func<NatsMsg<byte[]>, ValueTask> handle) =>
        Run(async () =>
        {
            try
            {
                await foreach (var msg in sub.Msgs.ReadAllAsync().ConfigureAwait(false))
                {
                    try
                    {
                        await handle(msg).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "error handling message on {Subject}", msg.Subject);
                    }
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
            {
                // subscription closed
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "subscription {Subject} ended with an error", sub.Subject);
            }
        });

    /// <summary>Ends a subscription, ignoring errors from an already closed connection.</summary>
    public static async ValueTask Close(INatsSub<byte[]>? sub)
    {
        if (sub is null)
        {
            return;
        }

        try
        {
            await sub.UnsubscribeAsync().ConfigureAwait(false);
            await sub.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // best effort
        }
    }
}
