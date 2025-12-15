namespace NServiceBus.Envelope.CloudEvents;

using System.Diagnostics.CodeAnalysis;
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
}

class CloudEventJsonStructuredEnvelopeHandler(CloudEventsMetrics metrics, CloudEventsConfiguration config) : IEnvelopeHandler
{
    static readonly ILog Log = LogManager.GetLogger<CloudEventJsonStructuredEnvelopeHandler>();
    static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    public (Dictionary<string, string> headers, ReadOnlyMemory<byte> body)? UnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders, ContextBag extensions, ReadOnlyMemory<byte> incomingBody)
    {
        var isStrict = config.EnvelopeUnwrappers.Find<CloudEventJsonStructuredEnvelopeUnwrapper>().EnvelopeHandlingMode == JsonStructureEnvelopeHandlingMode.Strict;

        Dictionary<string, JsonProperty>? receivedCloudEvent = isStrict
            ? StrictHandler.DeserializeOrThrow(incomingHeaders, incomingBody, metrics)
            : PermissiveHandler.DeserializeOrThrow(incomingBody, metrics);

        return receivedCloudEvent == null
            ? null
            : (ExtractHeaders(incomingHeaders, receivedCloudEvent), ExtractBody(receivedCloudEvent));
    }

    Dictionary<string, string> ExtractHeaders(IDictionary<string, string> existingHeaders, Dictionary<string, JsonProperty> receivedCloudEvent)
    {
        var headersCopy = existingHeaders.ToDictionary(k => k.Key, k => k.Value);

        foreach (var kvp in receivedCloudEvent)
        {
            if (
                CloudEventJsonStructuredConstants.HeadersToIgnore.Contains(kvp.Key)
                || kvp.Value.Value.ValueKind == JsonValueKind.Undefined
                || kvp.Value.Value.ValueKind == JsonValueKind.Null
            )
            {
                continue;
            }

            headersCopy[kvp.Key] = kvp.Value.Value.ValueKind == JsonValueKind.String
                ? kvp.Value.Value.GetString()!
                : kvp.Value.Value.GetRawText();
        }

        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.IdProperty, out var id))
        {
            headersCopy[Headers.MessageId] = id;
        }

        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.SourceProperty, out var source))
        {
            headersCopy[Headers.ReplyToAddress] = source;
        }

        headersCopy[Headers.EnclosedMessageTypes] = ExtractType(receivedCloudEvent);

        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.TimeProperty, out var timeValue))
        {
            if (!string.IsNullOrEmpty(timeValue) && timeValue != CloudEventJsonStructuredConstants.NullLiteral)
            {
                /*
                 * If what comes in is something similar to "2018-04-05T17:31:00Z", compliant with the CloudEvents spec
                 * and ISO 8601, NServiceBus will not be happy and later in the pipeline there will be a parsing exception
                 */
                headersCopy[Headers.TimeSent] = DateTimeOffsetHelper.ToWireFormattedString(DateTimeOffset.Parse(timeValue));
            }
        }

        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.DataContentTypeProperty, out var dataContentType))
        {
            headersCopy[Headers.ContentType] = dataContentType;
        }
        else
        {
            headersCopy[Headers.ContentType] = CloudEventJsonStructuredConstants.JsonContentType;
        }

        return headersCopy;
    }

    string ExtractType(Dictionary<string, JsonProperty> receivedCloudEvent)
    {
        var cloudEventType = receivedCloudEvent[CloudEventJsonStructuredConstants.TypeProperty].Value.GetString()!;
        return config.TypeMappings.TryGetValue(cloudEventType, out var typeMapping)
            ? string.Join(',', typeMapping)
            : cloudEventType;
    }

    static ReadOnlyMemory<byte> ExtractBody(Dictionary<string, JsonProperty> receivedCloudEvent)
    {
        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.DataBase64Property, out var base64Body))
        {
            return new ReadOnlyMemory<byte>(Convert.FromBase64String(base64Body));
        }

        if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataProperty, out var data))
        {
            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataContentTypeProperty,
                    out var property) && !property.Value.GetString()!.EndsWith(CloudEventJsonStructuredConstants.JsonSuffix))
            {
                return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(
                    data.Value.GetString()!));
            }

            if (data.Value.ValueKind is not JsonValueKind.Undefined and not JsonValueKind.Null)
            {
                return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(
                    data.Value.GetRawText()));
            }
        }

        return new ReadOnlyMemory<byte>();
    }

    static bool TryGetHeader(Dictionary<string, JsonProperty> receivedCloudEvent, string header, [MaybeNullWhen(false)] out string result)
    {
        if (receivedCloudEvent.TryGetValue(header, out var value)
            && value.Value.ValueKind != JsonValueKind.Undefined
            && value.Value.ValueKind != JsonValueKind.Null)
        {
            result = value.Value.GetString()!;
            return true;
        }

        result = null;
        return false;
    }

    static Dictionary<string, JsonProperty> ToCaseInsensitiveDictionary(JsonDocument receivedCloudEvent) =>
        receivedCloudEvent.RootElement.EnumerateObject()
            .ToDictionary(p => p.Name.ToLowerInvariant(), p => p,
                StringComparer.OrdinalIgnoreCase);

    static class StrictHandler
    {
        static readonly string[] RequiredProperties = [
            CloudEventJsonStructuredConstants.IdProperty,
            CloudEventJsonStructuredConstants.SourceProperty,
            CloudEventJsonStructuredConstants.TypeProperty
        ];

        internal static Dictionary<string, JsonProperty>? DeserializeOrThrow(IDictionary<string, string> incomingHeaders,
            ReadOnlyMemory<byte> body, CloudEventsMetrics metrics)
        {
            if (!HasCorrectContentTypeHeader(incomingHeaders))
            {
                return null;
            }

            var receivedCloudEvent = JsonSerializer.Deserialize<JsonDocument>(body.Span, Options);

            if (receivedCloudEvent == null)
            {
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED);
                throw new NotSupportedException("Couldn't deserialize the message into a cloud event");
            }

            Dictionary<string, JsonProperty> caseInsensitiveProperties = ToCaseInsensitiveDictionary(receivedCloudEvent);
            ThrowIfInvalidCloudEventAndRecordMetrics(caseInsensitiveProperties, metrics);
            return caseInsensitiveProperties;
        }

        static bool HasCorrectContentTypeHeader(IDictionary<string, string> incomingHeaders) =>
            incomingHeaders.TryGetValue(Headers.ContentType, out var value) &&
            (value == CloudEventJsonStructuredConstants.SupportedContentType || value.Contains(CloudEventJsonStructuredConstants.SupportedContentType));

        static void ThrowIfInvalidCloudEventAndRecordMetrics(Dictionary<string, JsonProperty> receivedCloudEvent, CloudEventsMetrics metrics)
        {
            foreach (var property in RequiredProperties)
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
    }

    static class PermissiveHandler
    {
        internal static Dictionary<string, JsonProperty>? DeserializeOrThrow(ReadOnlyMemory<byte> body, CloudEventsMetrics metrics)
        {
            var receivedCloudEvent = JsonSerializer.Deserialize<JsonDocument>(body.Span, Options);

            if (receivedCloudEvent == null)
            {
                return null;
            }

            Dictionary<string, JsonProperty> caseInsensitiveProperties = ToCaseInsensitiveDictionary(receivedCloudEvent);


            if (!caseInsensitiveProperties.TryGetValue(CloudEventJsonStructuredConstants.TypeProperty, out _))
            {
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED);
                return null;
            }

            RecordMetrics(caseInsensitiveProperties, metrics);

            return caseInsensitiveProperties;
        }

        static void RecordMetrics(Dictionary<string, JsonProperty> receivedCloudEvent, CloudEventsMetrics metrics)
        {
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
        }
    }
}