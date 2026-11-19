using System.Globalization;
using Microsoft.Extensions.Logging;

namespace TransactRx.NatsAgent.Internal;

/// <summary>
/// The default logger when the application supplies no <see cref="ILoggerFactory"/>: one line
/// per entry on stdout, like the Go library, so operational lines (for example
/// <c>IDT_METRIC event=...</c>) are never silently dropped.
/// </summary>
internal sealed class StdoutLoggerFactory : ILoggerFactory
{
    public static readonly StdoutLoggerFactory Instance = new();

    public ILogger CreateLogger(string categoryName) => StdoutLogger.Instance;

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public void Dispose()
    {
    }
}

internal sealed class StdoutLogger : ILogger
{
    public static readonly StdoutLogger Instance = new();

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        var line = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss", CultureInfo.InvariantCulture) + " " + formatter(state, exception);
        if (exception is not null)
        {
            line += ": " + exception.Message;
        }

        Console.Out.WriteLine(line);
    }
}

/// <summary>Prefixes every entry, e.g. with a request's message id.</summary>
internal sealed class PrefixLogger(ILogger inner, string prefix) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => inner.BeginScope(state);

    public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        inner.Log(logLevel, eventId, state, exception, (s, e) => prefix + formatter(s, e));
}

internal static class GoDuration
{
    /// <summary>Formats like Go's time.Duration rounded to a second: "0s", "42s", "3m5s", "2h0m7s".</summary>
    public static string Format(TimeSpan d)
    {
        var total = (long)Math.Round(d.TotalSeconds, MidpointRounding.AwayFromZero);
        if (total <= 0)
        {
            return "0s";
        }

        var h = total / 3600;
        var m = total % 3600 / 60;
        var s = total % 60;
        if (h > 0)
        {
            return $"{h}h{m}m{s}s";
        }

        return m > 0 ? $"{m}m{s}s" : $"{s}s";
    }

    /// <summary>Formats a duration for messages the way Go prints it: "45s", "1m30s", "250ms".</summary>
    public static string FormatPrecise(TimeSpan d)
    {
        if (d < TimeSpan.FromSeconds(1))
        {
            return d.TotalMilliseconds.ToString("0.###", CultureInfo.InvariantCulture) + "ms";
        }

        var whole = TimeSpan.FromSeconds(Math.Floor(d.TotalSeconds));
        var frac = d - whole;
        var text = Format(whole);
        if (frac > TimeSpan.Zero)
        {
            // Go prints fractional seconds on the seconds component: 1m30.5s
            var secs = (whole.TotalSeconds % 60) + frac.TotalSeconds;
            text = text[..(text.LastIndexOfAny(['m', 'h']) + 1)] + secs.ToString("0.###", CultureInfo.InvariantCulture) + "s";
        }

        return text;
    }
}
