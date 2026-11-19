using System.Globalization;
using System.Text;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using TransactRx.NatsAgent.Internal;

namespace TransactRx.NatsAgent.Wire;

/// <summary>
/// (v1.3) Display context about the person behind a turn: who they are called and what their
/// clock says, so an agent can greet them and read "today" in their time zone. It is DISPLAY
/// ONLY — never use it for authorization; identity comes from the X-TRX-IDT token (§5.1).
/// <para>
/// Callers (UIs, relays, orchestrators) set it; agents read it through
/// <see cref="UserContextExtensions.Normalize"/>, which drops anything invalid instead of
/// failing the turn.
/// </para>
/// </summary>
public sealed record UserContext
{
    /// <summary>IANA name, e.g. "America/New_York".</summary>
    [JsonPropertyName("timeZone")]
    public string? TimeZone { get; init; }

    /// <summary>Display name, e.g. "Jane Doe".</summary>
    [JsonPropertyName("userName")]
    public string? UserName { get; init; }

    /// <summary>BCP 47 tag, e.g. "en-US".</summary>
    [JsonPropertyName("locale")]
    public string? Locale { get; init; }
}

/// <summary>
/// Operations on a <see cref="UserContext"/>. All of them are safe to call on <c>null</c>
/// (an absent context), like the Go methods they mirror.
/// </summary>
public static partial class UserContextExtensions
{
    internal const int MaxUserNameRunes = 80;
    private const int MaxTimeZoneLen = 64;
    private const int MaxLocaleLen = 35;

    [GeneratedRegex(@"^[A-Za-z][A-Za-z0-9_+\-]*(/[A-Za-z0-9_+\-]+)*$")]
    private static partial Regex TimeZoneRegex();

    [GeneratedRegex(@"^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$")]
    private static partial Regex LocaleRegex();

    /// <summary>
    /// Returns a cleaned copy: an unknown time zone, a malformed locale, and every character
    /// of the name outside letters, digits, spaces and <c>. ' - ,</c> are dropped; whitespace
    /// is collapsed and the name capped at 80 characters. Returns null when nothing usable is
    /// left.
    /// </summary>
    public static UserContext? Normalize(this UserContext? u)
    {
        if (u is null)
        {
            return null;
        }

        var tz = CleanTimeZone(u.TimeZone);
        var name = CleanUserName(u.UserName);
        var locale = CleanLocale(u.Locale);
        if (tz is null && name is null && locale is null)
        {
            return null;
        }

        return new UserContext { TimeZone = tz, UserName = name, Locale = locale };
    }

    /// <summary>The user's time zone, or UTC when unknown or invalid.</summary>
    public static TimeZoneInfo Zone(this UserContext? u)
    {
        var tz = CleanTimeZone(u?.TimeZone);
        return tz is not null && TimeZoneInfo.TryFindSystemTimeZoneById(tz, out var zone) ? zone : TimeZoneInfo.Utc;
    }

    /// <summary>
    /// <paramref name="instant"/> on the user's clock: the same moment, with the offset of the
    /// user's time zone (UTC when unknown or invalid).
    /// </summary>
    public static DateTimeOffset ToLocalTime(this UserContext? u, DateTimeOffset instant) =>
        TimeZoneInfo.ConvertTime(instant, u.Zone());

    /// <summary>
    /// The standard sentence every agent puts in its system prompt so the model knows who is
    /// asking and what "today" means for them, e.g.:
    /// <para>
    /// The user asking is Jane Doe (time zone America/New_York). Their current local time is
    /// Monday, October 19, 2026 9:30 PM EDT; UTC is 2026-10-20 01:30 UTC.
    /// </para>
    /// </summary>
    public static string Line(this UserContext? u, DateTimeOffset now)
    {
        var c = u.Normalize();
        var utc = now.UtcDateTime.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC";
        if (c?.TimeZone is null)
        {
            var asking = c?.UserName is null ? "" : "The user asking is " + c.UserName + ". ";
            return asking + "The user's time zone is unknown; the current time is " + utc + ".";
        }

        var zone = c.Zone();
        var localTime = TimeZoneInfo.ConvertTime(now, zone);
        var local = localTime.ToString("dddd, MMMM d, yyyy h:mm tt", CultureInfo.InvariantCulture)
            + " " + TimeZoneAbbreviation.For(c.TimeZone, localTime.Offset);
        var who = c.UserName is null
            ? "The user's time zone is " + c.TimeZone + "."
            : "The user asking is " + c.UserName + " (time zone " + c.TimeZone + ").";
        return who + " Their current local time is " + local + "; UTC is " + utc + ".";
    }

    private static string? CleanTimeZone(string? tz)
    {
        tz = tz?.Trim();

        // "Local" would silently mean the agent's own zone. Only IANA names the system's time
        // zone database knows count; Windows ids and offset spellings are not IANA names.
        if (string.IsNullOrEmpty(tz) || tz == "Local" || tz.Length > MaxTimeZoneLen || !TimeZoneRegex().IsMatch(tz))
        {
            return null;
        }

        return TimeZoneInfo.TryFindSystemTimeZoneById(tz, out var zone) && zone.HasIanaId ? tz : null;
    }

    private static string? CleanUserName(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return null;
        }

        var b = new StringBuilder(name.Length);
        foreach (var r in name.EnumerateRunes())
        {
            if (Rune.IsLetter(r) || IsMark(r) || Rune.IsDigit(r) || r.Value is '.' or '\'' or '-' or ',')
            {
                b.Append(r.ToString());
            }
            else if (Rune.IsWhiteSpace(r))
            {
                b.Append(' ');
            }
        }

        var cleaned = string.Join(' ', b.ToString().Split(' ', StringSplitOptions.RemoveEmptyEntries));
        var runes = cleaned.EnumerateRunes().ToArray();
        if (runes.Length > MaxUserNameRunes)
        {
            cleaned = string.Concat(runes.Take(MaxUserNameRunes).Select(r => r.ToString())).Trim();
        }

        return cleaned.Length == 0 ? null : cleaned;
    }

    private static bool IsMark(Rune r) => Rune.GetUnicodeCategory(r) is
        UnicodeCategory.NonSpacingMark or UnicodeCategory.SpacingCombiningMark or UnicodeCategory.EnclosingMark;

    private static string? CleanLocale(string? locale)
    {
        locale = locale?.Trim();
        if (string.IsNullOrEmpty(locale) || locale.Length > MaxLocaleLen || !LocaleRegex().IsMatch(locale))
        {
            return null;
        }

        return locale;
    }
}
