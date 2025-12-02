namespace NServiceBus.Envelope.CloudEvents;

using System.Text;
using System.Text.Json;
using Extensibility;
using Logging;

class CloudEventJsonStructuredEnvelopeHandler(CloudEventsMetrics metrics) : IEnvelopeHandler
{
    static readonly ILog Log = LogManager.GetLogger<CloudEventJsonStructuredEnvelopeHandler>();

    const string TYPE_PROPERTY = "type";
    const string DATA_CONTENT_TYPE_PROPERTY = "datacontenttype";
    const string DATA_PROPERTY = "data";
    const string DATA_BASE64_PROPERTY = "data_base64";
    const string ID_PROPERTY = "id";
    const string SOURCE_PROPERTY = "source";
    const string TIME_PROPERTY = "time";
    const string VERSION_PROPERTY = "specversion";
    const string JSON_SUFFIX = "json";
    const string SUPPORTED_VERSION = "1.0";
    const string SUPPORTED_CONTENT_TYPE = "application/cloudevents+json";

    static readonly HashSet<string> HEADERS_TO_IGNORE = [DATA_PROPERTY, DATA_BASE64_PROPERTY];
    static readonly string[] REQUIRED_PROPERTIES = [ID_PROPERTY, SOURCE_PROPERTY, TYPE_PROPERTY, DATA_CONTENT_TYPE_PROPERTY];

    static readonly JsonSerializerOptions options = new() { PropertyNameCaseInsensitive = true };

    public (Dictionary<string, string> headers, ReadOnlyMemory<byte> body) UnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders, ContextBag extensions, ReadOnlyMemory<byte> incomingBody)
    {
        ThrowIfInvalidMessage(incomingHeaders);
        JsonDocument receivedCloudEvent = DeserializeOrThrow(incomingBody);
        var headers = ExtractHeaders(incomingHeaders, receivedCloudEvent);
        var body = ExtractBody(receivedCloudEvent);
        return (headers, body);
    }

    static ReadOnlyMemory<byte> ExtractBody(JsonDocument receivedCloudEvent)
    {
        if (receivedCloudEvent.RootElement.TryGetProperty(DATA_BASE64_PROPERTY, out var base64Body))
        {
            return ExtractBodyFromBase64(base64Body);
        }

        return ExtractBodyFromProperty(receivedCloudEvent);
    }

    static ReadOnlyMemory<byte> ExtractBodyFromProperty(JsonDocument receivedCloudEvent)
    {
        if (receivedCloudEvent.RootElement.TryGetProperty(DATA_CONTENT_TYPE_PROPERTY, out var property) && property.GetString()!.EndsWith(JSON_SUFFIX))
        {
            return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(receivedCloudEvent.RootElement.GetProperty(DATA_PROPERTY).GetRawText()));
        }

        return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(receivedCloudEvent.RootElement.GetProperty(DATA_PROPERTY).GetString()!));
    }

    static ReadOnlyMemory<byte> ExtractBodyFromBase64(JsonElement base64Body) => new(Convert.FromBase64String(base64Body.GetString()!));

    static Dictionary<string, string> ExtractHeaders(IDictionary<string, string> existingHeaders, JsonDocument receivedCloudEvent)
    {
        var headersCopy = existingHeaders.ToDictionary(k => k.Key, k => k.Value);

        foreach (var kvp in receivedCloudEvent
                     .RootElement
                     .EnumerateObject()
                     .Where(p => !HEADERS_TO_IGNORE.Contains(p.Name))
                     .Where(p => p.Value.ValueKind != JsonValueKind.Null))
        {
            headersCopy[kvp.Name] = kvp.Value.ValueKind == JsonValueKind.String
                ? kvp.Value.GetString()!
                : kvp.Value.GetRawText();
        }

        headersCopy[Headers.MessageId] = ExtractId(receivedCloudEvent);
        headersCopy[Headers.ReplyToAddress] = ExtractSource(receivedCloudEvent);
        if (receivedCloudEvent.RootElement.TryGetProperty(TIME_PROPERTY, out var time))
        {
            headersCopy[Headers.TimeSent] = time.GetString()!;
        }

        return headersCopy;
    }

    static string ExtractId(JsonDocument receivedCloudEvent) => ExtractHeader(receivedCloudEvent, ID_PROPERTY);

    static string ExtractSource(JsonDocument receivedCloudEvent) => ExtractHeader(receivedCloudEvent, SOURCE_PROPERTY);

    static string ExtractHeader(JsonDocument receivedCloudEvent, string property) => receivedCloudEvent.RootElement.GetProperty(property).GetString()!;

    void ThrowIfInvalidCloudEvent(JsonDocument? receivedCloudEvent)
    {
        if (receivedCloudEvent == null)
        {
            metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED);
            throw new NotSupportedException("Couldn't deserialize the message into a cloud event");
        }

        foreach (var property in REQUIRED_PROPERTIES)
        {
            if (!receivedCloudEvent.RootElement.TryGetProperty(property, out _))
            {
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED);
                throw new NotSupportedException($"Message lacks {property} property");
            }
        }

        if (!receivedCloudEvent.RootElement.TryGetProperty(DATA_BASE64_PROPERTY, out _) &&
            !receivedCloudEvent.RootElement.TryGetProperty(DATA_PROPERTY, out _))
        {
            metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED);
            throw new NotSupportedException($"Message lacks both {DATA_PROPERTY} and {DATA_BASE64_PROPERTY} property");
        }

        metrics.RecordValidMessage(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED);

        if (receivedCloudEvent.RootElement.TryGetProperty(VERSION_PROPERTY, out var version))
        {
            var versionValue = version.GetString();

            if (versionValue != SUPPORTED_VERSION)
            {
                metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED, versionValue);

                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}",
                        versionValue, receivedCloudEvent.RootElement.GetProperty(ID_PROPERTY).GetString());
                }
            }
            else
            {
                metrics.RecordUnexpectedVersion(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED, SUPPORTED_VERSION);
            }
        }
        else
        {
            metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED, null);

            if (Log.IsWarnEnabled)
            {
                Log.WarnFormat("CloudEvent version property is missing for message id {0}", receivedCloudEvent.RootElement.GetProperty(ID_PROPERTY).GetString());
            }
        }
    }

    JsonDocument DeserializeOrThrow(ReadOnlyMemory<byte> body)
    {
        var receivedCloudEvent = JsonSerializer.Deserialize<JsonDocument>(body.Span, options);
        ThrowIfInvalidCloudEvent(receivedCloudEvent);
        return receivedCloudEvent!;
    }

    static void ThrowIfInvalidMessage(IDictionary<string, string> headers)
    {
        if (headers.TryGetValue(Headers.ContentType, out var value))
        {
            if (value != SUPPORTED_CONTENT_TYPE)
            {
                throw new NotSupportedException($"Unsupported content type {value}");
            }
        }
        else
        {
            throw new NotSupportedException("Missing content type");
        }
    }

    public bool CanUnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders, ContextBag extensions, ReadOnlyMemory<byte> incomingBody) =>
        incomingHeaders.TryGetValue(Headers.ContentType, out var value) && value == SUPPORTED_CONTENT_TYPE;
}