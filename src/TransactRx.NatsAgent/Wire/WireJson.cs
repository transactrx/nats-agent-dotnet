using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace TransactRx.NatsAgent.Wire;

/// <summary>
/// JSON conventions of the wire protocol: camelCase names (declared per property), absent and
/// null are equivalent, unknown fields are ignored.
/// </summary>
public static class WireJson
{
    /// <summary>Serializer options every wire message is read and written with.</summary>
    public static readonly JsonSerializerOptions Options = new()
    {
        // Go's encoding/json matches field names case-insensitively; stay as lenient.
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        // The wire is UTF-8 JSON over NATS, never embedded in HTML.
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    /// <summary>The JSON value <c>{}</c>.</summary>
    public static readonly JsonElement EmptyObject = JsonSerializer.SerializeToElement(new Dictionary<string, object>());

    public static byte[] Serialize<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, Options);

    public static string SerializeToString<T>(T value) => JsonSerializer.Serialize(value, Options);

    /// <summary>Deserializes; throws <see cref="JsonException"/> on malformed input (including empty).</summary>
    public static T? Deserialize<T>(ReadOnlySpan<byte> utf8) => JsonSerializer.Deserialize<T>(utf8, Options);

    /// <summary>Deserializes; false on malformed input or a JSON null.</summary>
    public static bool TryDeserialize<T>(ReadOnlySpan<byte> utf8, out T value)
        where T : class
    {
        try
        {
            var v = JsonSerializer.Deserialize<T>(utf8, Options);
            value = v!;
            return v is not null;
        }
        catch (JsonException)
        {
            value = null!;
            return false;
        }
    }

    /// <summary>Converts any JSON-serializable value into a detached <see cref="JsonElement"/>.</summary>
    public static JsonElement ToElement<T>(T value) => JsonSerializer.SerializeToElement(value, Options);

    /// <summary>Parses JSON text into a detached <see cref="JsonElement"/>.</summary>
    public static JsonElement Parse(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }
}
