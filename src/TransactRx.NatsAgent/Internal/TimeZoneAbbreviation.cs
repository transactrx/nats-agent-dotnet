using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace TransactRx.NatsAgent.Internal;

/// <summary>
/// The short name of a time zone at a given offset ("EDT", "CET", "+03"), which
/// <see cref="TimeZoneInfo"/> does not expose. Read from the POSIX rule line that ends every
/// compiled IANA zone file (TZif version 2 and later, RFC 8536 §3.3), e.g.
/// <c>EST5EDT,M3.2.0,M11.1.0</c>. Where no zone file is available (Windows) the numeric form
/// the IANA database itself uses for unnamed zones is returned, e.g. "-04".
/// </summary>
internal static partial class TimeZoneAbbreviation
{
    private static readonly ConcurrentDictionary<string, Rule?> Rules = new(StringComparer.Ordinal);

    // name, then the offset west of UTC, then an optional daylight name.
    [GeneratedRegex(@"^(?<std><[^>]+>|[A-Za-z]{3,})(?<off>[+-]?\d{1,3}(:\d{1,2}(:\d{1,2})?)?)(?<dst><[^>]+>|[A-Za-z]{3,})?")]
    private static partial Regex PosixTz();

    internal sealed record Rule(string Std, TimeSpan StdOffset, string? Dst);

    /// <param name="ianaId">A validated IANA zone id (letters, digits, <c>_ + - /</c> only).</param>
    /// <param name="offset">The zone's UTC offset at the moment being formatted.</param>
    public static string For(string ianaId, TimeSpan offset)
    {
        var rule = Rules.GetOrAdd(ianaId, Load);
        if (rule is not null)
        {
            if (offset == rule.StdOffset)
            {
                return rule.Std;
            }

            if (rule.Dst is not null)
            {
                return rule.Dst;
            }
        }

        return offset == TimeSpan.Zero && ianaId is "UTC" or "Etc/UTC" ? "UTC" : Numeric(offset);
    }

    internal static string Numeric(TimeSpan offset)
    {
        var sign = offset < TimeSpan.Zero ? "-" : "+";
        var abs = offset.Duration();
        var text = sign + abs.Hours.ToString("00", CultureInfo.InvariantCulture);
        return abs.Minutes == 0 ? text : text + abs.Minutes.ToString("00", CultureInfo.InvariantCulture);
    }

    internal static Rule? Parse(string footer)
    {
        var m = PosixTz().Match(footer);
        if (!m.Success)
        {
            return null;
        }

        // POSIX offsets count west of UTC: EST5 is UTC-5.
        var parts = m.Groups["off"].Value.TrimStart('+', '-').Split(':');
        var west = new TimeSpan(
            int.Parse(parts[0], CultureInfo.InvariantCulture),
            parts.Length > 1 ? int.Parse(parts[1], CultureInfo.InvariantCulture) : 0,
            parts.Length > 2 ? int.Parse(parts[2], CultureInfo.InvariantCulture) : 0);
        if (m.Groups["off"].Value.StartsWith('-'))
        {
            west = -west;
        }

        return new Rule(Name(m.Groups["std"].Value), -west, m.Groups["dst"].Success ? Name(m.Groups["dst"].Value) : null);

        static string Name(string raw) => raw.Trim('<', '>');
    }

    private static Rule? Load(string ianaId)
    {
        foreach (var dir in ZoneDirectories())
        {
            try
            {
                var path = Path.Combine(dir, ianaId);
                if (!File.Exists(path))
                {
                    continue;
                }

                var data = File.ReadAllBytes(path);
                // "TZif" + version; the rule line exists from version 2 on, between the last
                // two newlines of the file.
                if (data.Length < 6 || data[0] != 'T' || data[1] != 'Z' || data[2] != 'i' || data[3] != 'f' || data[4] < '2' || data[^1] != '\n')
                {
                    return null;
                }

                var start = Array.LastIndexOf(data, (byte)'\n', data.Length - 2);
                return start < 0 ? null : Parse(Encoding.ASCII.GetString(data, start + 1, data.Length - start - 2));
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or FormatException or OverflowException or ArgumentException)
            {
                return null;
            }
        }

        return null;
    }

    private static IEnumerable<string> ZoneDirectories()
    {
        var tzdir = Environment.GetEnvironmentVariable("TZDIR");
        if (!string.IsNullOrEmpty(tzdir))
        {
            yield return tzdir;
        }

        yield return "/usr/share/zoneinfo";
        yield return "/usr/lib/zoneinfo";
        yield return "/usr/share/lib/zoneinfo";
        yield return "/etc/zoneinfo";
    }
}
