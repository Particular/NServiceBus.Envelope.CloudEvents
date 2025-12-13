namespace NServiceBus.Envelope.CloudEvents;

using System.Text;
using System.Text.Json;
using Extensibility;
using Logging;

static class CloudEventJsonStructuredConstants
{
    internal const string TypeProperty = "type";
    internal const string DataContentTypeProperty = "datacontenttype";
    internal const string DataProperty = "data";
    internal const string DataBase64Property = "data_base64";
    internal const string IdProperty = "id";
    internal const string SourceProperty = "source";
    internal const string TimeProperty = "time";
    internal const string VersionProperty = "specversion";
    internal const string JsonSuffix = "json";
    internal const string SupportedVersion = "1.0";
    internal const string SupportedContentType = "application/cloudevents+json";
    internal const string JsonContentType = "application/json";
    internal const string NullLiteral = "null";

    internal static readonly HashSet<string> HeadersToIgnore = [DataProperty, DataBase64Property];
    internal static readonly string[] RequiredProperties = [IdProperty, SourceProperty, TypeProperty];
}

class CloudEventJsonStructuredEnvelopeHandler(CloudEventsMetrics metrics, CloudEventsConfiguration config) : IEnvelopeHandler
{
    static readonly ILog Log = LogManager.GetLogger<CloudEventJsonStructuredEnvelopeHandler>();

    static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    public (Dictionary<string, string> headers, ReadOnlyMemory<byte> body) UnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders, ContextBag extensions, ReadOnlyMemory<byte> incomingBody)
    {
        var mode = config.EnvelopeUnwrappers.Find<CloudEventJsonStructuredEnvelopeUnwrapper>().EnvelopeHandlingMode;
        if (mode == JsonStructureEnvelopeHandlingMode.Strict)
        {
            // behavior for strict
        }
        else
        {
            // behavior for permissive
        }

        ThrowIfInvalidMessage(incomingHeaders);
        Dictionary<string, JsonProperty> receivedCloudEvent = DeserializeOrThrow(incomingBody);
        var headers = ExtractHeaders(incomingHeaders, receivedCloudEvent);
        var body = ExtractBody(receivedCloudEvent);
        return (headers, body);
    }

    static ReadOnlyMemory<byte> ExtractBody(Dictionary<string, JsonProperty> receivedCloudEvent)
    {
        if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataBase64Property, out var base64Body))
        {
            return ExtractBodyFromBase64(base64Body);
        }

        return ExtractBodyFromProperty(receivedCloudEvent);
    }

    static ReadOnlyMemory<byte> ExtractBodyFromProperty(Dictionary<string, JsonProperty> receivedCloudEvent)
    {
        if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataContentTypeProperty, out var property) && !property.Value.GetString()!.EndsWith(CloudEventJsonStructuredConstants.JsonSuffix))
        {
            return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(receivedCloudEvent[CloudEventJsonStructuredConstants.DataProperty].Value.GetString()!));
        }

        return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(receivedCloudEvent[CloudEventJsonStructuredConstants.DataProperty].Value.GetRawText()));
    }

    static ReadOnlyMemory<byte> ExtractBodyFromBase64(JsonProperty base64Body) => new(Convert.FromBase64String(base64Body.Value.GetString()!));

    Dictionary<string, string> ExtractHeaders(IDictionary<string, string> existingHeaders, Dictionary<string, JsonProperty> receivedCloudEvent)
    {
        var headersCopy = existingHeaders.ToDictionary(k => k.Key, k => k.Value);

        foreach (var kvp in receivedCloudEvent)
        {
            if (CloudEventJsonStructuredConstants.HeadersToIgnore.Contains(kvp.Key) || kvp.Value.Value.ValueKind == JsonValueKind.Null)
            {
                continue;
            }

            headersCopy[kvp.Key] = kvp.Value.Value.ValueKind == JsonValueKind.String
                ? kvp.Value.Value.GetString()!
                : kvp.Value.Value.GetRawText();
        }

        headersCopy[Headers.MessageId] = ExtractId(receivedCloudEvent);
        headersCopy[Headers.ReplyToAddress] = ExtractSource(receivedCloudEvent);
        headersCopy[Headers.EnclosedMessageTypes] = ExtractType(receivedCloudEvent);
        if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.TimeProperty, out var time) && time.Value.ValueKind != JsonValueKind.Null)
        {
            var timeValue = time.Value.GetString()!;
            if (!string.IsNullOrEmpty(timeValue) && timeValue != CloudEventJsonStructuredConstants.NullLiteral)
            {
                /*
                 * If what comes in is something similar to "2018-04-05T17:31:00Z", compliant with the CloudEvents spec
                 * and ISO 8601, NServiceBus will not be happy and later in the pipeline there will be a parsing exception
                 */
                headersCopy[Headers.TimeSent] = DateTimeOffsetHelper.ToWireFormattedString(DateTimeOffset.Parse(timeValue));
            }
        }

        if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataContentTypeProperty, out var dataContentType))
        {
            headersCopy[Headers.ContentType] = dataContentType.Value.GetString()!;
        }
        else
        {
            headersCopy[Headers.ContentType] = CloudEventJsonStructuredConstants.JsonContentType;
        }

        return headersCopy;
    }

    static string ExtractId(Dictionary<string, JsonProperty> receivedCloudEvent) => ExtractHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.IdProperty);

    string ExtractType(Dictionary<string, JsonProperty> receivedCloudEvent)
    {
        var cloudEventType = ExtractHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.TypeProperty);
        return config.TypeMappings.TryGetValue(cloudEventType, out var typeMapping)
            ? string.Join(',', typeMapping)
            : cloudEventType;
    }
    static string ExtractSource(Dictionary<string, JsonProperty> receivedCloudEvent) => ExtractHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.SourceProperty);

    static string ExtractHeader(Dictionary<string, JsonProperty> receivedCloudEvent, string property) => receivedCloudEvent[property].Value.GetString()!;

    void ThrowIfInvalidCloudEvent(Dictionary<string, JsonProperty> receivedCloudEvent)
    {

        foreach (var property in CloudEventJsonStructuredConstants.RequiredProperties)
        {
            if (!receivedCloudEvent.TryGetValue(property, out _))
            {
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED);
                throw new NotSupportedException($"Message lacks {property} property");
            }
        }

        if (!receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataBase64Property, out _) &&
            !receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataProperty, out _))
        {
            metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED);
            throw new NotSupportedException($"Message lacks both {CloudEventJsonStructuredConstants.DataProperty} and {CloudEventJsonStructuredConstants.DataBase64Property} property");
        }

        metrics.RecordValidMessage(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED);

        if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.VersionProperty, out var version))
        {
            var versionValue = version.Value.GetString();

            if (versionValue != CloudEventJsonStructuredConstants.SupportedVersion)
            {
                metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED, versionValue);

                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}",
                        versionValue, receivedCloudEvent[CloudEventJsonStructuredConstants.IdProperty].Value.GetString());
                }
            }
            else
            {
                metrics.RecordUnexpectedVersion(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED, CloudEventJsonStructuredConstants.SupportedVersion);
            }
        }
        else
        {
            metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED, null);

            if (Log.IsWarnEnabled)
            {
                Log.WarnFormat("CloudEvent version property is missing for message id {0}", receivedCloudEvent[CloudEventJsonStructuredConstants.IdProperty].Value.GetString());
            }
        }
    }

    Dictionary<string, JsonProperty> DeserializeOrThrow(ReadOnlyMemory<byte> body)
    {
        var receivedCloudEvent = JsonSerializer.Deserialize<JsonDocument>(body.Span, Options);

        if (receivedCloudEvent == null)
        {
            metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED);
            throw new NotSupportedException("Couldn't deserialize the message into a cloud event");
        }

        Dictionary<string, JsonProperty> caseInsensitiveProperties = ToCaseInsensitiveDictionary(receivedCloudEvent);
        ThrowIfInvalidCloudEvent(caseInsensitiveProperties);
        return caseInsensitiveProperties;
    }

    static Dictionary<string, JsonProperty> ToCaseInsensitiveDictionary(JsonDocument receivedCloudEvent)
    {
        return receivedCloudEvent.RootElement.EnumerateObject()
            .ToDictionary(p => p.Name.ToLowerInvariant(), p => p,
                StringComparer.OrdinalIgnoreCase);
    }

    static void ThrowIfInvalidMessage(IDictionary<string, string> headers)
    {
        if (headers.TryGetValue(Headers.ContentType, out var value))
        {
            if (value != CloudEventJsonStructuredConstants.SupportedContentType && !value.Contains(CloudEventJsonStructuredConstants.SupportedContentType))
            {
                throw new NotSupportedException($"Unsupported content type {value}");
            }
        }
        else
        {
            throw new NotSupportedException("Missing content type");
        }
    }

    public bool CanUnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders,
        ContextBag extensions, ReadOnlyMemory<byte> incomingBody) =>
        incomingHeaders.TryGetValue(Headers.ContentType, out var value) &&
        (value == CloudEventJsonStructuredConstants.SupportedContentType || value.Contains(CloudEventJsonStructuredConstants.SupportedContentType));
}