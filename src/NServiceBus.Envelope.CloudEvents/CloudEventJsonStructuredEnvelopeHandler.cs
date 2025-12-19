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
            ? StrictHandler.DeserializeOrThrow(nativeMessageId, incomingHeaders, incomingBody, metrics)
            : PermissiveHandler.DeserializeOrThrow(nativeMessageId, incomingBody, metrics);

        return receivedCloudEvent == null
            ? null
            : (ExtractHeaders(nativeMessageId, incomingHeaders, receivedCloudEvent), ExtractBody(nativeMessageId, receivedCloudEvent));
    }

    Dictionary<string, string> ExtractHeaders(string nativeMessageId, IDictionary<string, string> existingHeaders,
        Dictionary<string, JsonProperty> receivedCloudEvent)
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

            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Extracted {headersCopy[kvp.Key]} for {kvp.Key} field for messageId {nativeMessageId}");
            }
        }

        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.IdProperty, out var id))
        {
            headersCopy[Headers.MessageId] = id;
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Extracted {headersCopy[Headers.MessageId]} for {CloudEventJsonStructuredConstants.IdProperty} field for messageId {nativeMessageId}");
            }
        }

        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.SourceProperty, out var source))
        {
            headersCopy[Headers.ReplyToAddress] = source;
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Extracted {headersCopy[Headers.ReplyToAddress]} for {CloudEventJsonStructuredConstants.SourceProperty} field for messageId {nativeMessageId}");
            }
        }

        headersCopy[Headers.EnclosedMessageTypes] = ExtractType(receivedCloudEvent);
        if (Log.IsDebugEnabled)
        {
            Log.Debug($"Extracted {headersCopy[Headers.EnclosedMessageTypes]} for {CloudEventJsonStructuredConstants.TypeProperty} field for messageId {nativeMessageId}");
        }

        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.TimeProperty, out var timeValue)
            && !string.IsNullOrEmpty(timeValue) && timeValue != CloudEventJsonStructuredConstants.NullLiteral)
        {
            /*
             * If what comes in is something similar to "2018-04-05T17:31:00Z", compliant with the CloudEvents spec
             * and ISO 8601, NServiceBus will not be happy and later in the pipeline there will be a parsing exception
             */
            headersCopy[Headers.TimeSent] = DateTimeOffsetHelper.ToWireFormattedString(DateTimeOffset.Parse(timeValue));
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Extracted {headersCopy[Headers.TimeSent]} for {CloudEventJsonStructuredConstants.TimeProperty} field for messageId {nativeMessageId}");
            }
        }
        else
        {
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"No time extracted for messageId {nativeMessageId}");
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
        if (Log.IsDebugEnabled)
        {
            Log.Debug($"Extracted {headersCopy[Headers.ContentType]} for {CloudEventJsonStructuredConstants.DataContentTypeProperty} field for messageId {nativeMessageId}");
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

    static ReadOnlyMemory<byte> ExtractBody(string nativeMessageId, Dictionary<string, JsonProperty> receivedCloudEvent)
    {
        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.DataBase64Property, out var base64Body))
        {
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Extracting inner body from {CloudEventJsonStructuredConstants.DataBase64Property} for message {nativeMessageId}");
            }
            return new ReadOnlyMemory<byte>(Convert.FromBase64String(base64Body));
        }

        if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataProperty, out var data))
        {
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Extracting inner body from {CloudEventJsonStructuredConstants.DataProperty} for message {nativeMessageId}");
            }

            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataContentTypeProperty,
                    out var property) && !property.Value.GetString()!.EndsWith(CloudEventJsonStructuredConstants.JsonSuffix))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Passing inner body as text for message {nativeMessageId}");
                }
                return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(
                    data.Value.GetString()!));
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Passing inner body as JSON for message {nativeMessageId}");
            }
            if (data.Value.ValueKind is not JsonValueKind.Undefined and not JsonValueKind.Null)
            {
                return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(
                    data.Value.GetRawText()));
            }
        }

        if (Log.IsDebugEnabled)
        {
            Log.Debug($"Empty inner body for message {nativeMessageId}");
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

        internal static Dictionary<string, JsonProperty>? DeserializeOrThrow(string nativeMessageId,
            IDictionary<string, string> incomingHeaders,
            ReadOnlyMemory<byte> body, CloudEventsMetrics metrics)
        {
            if (!HasCorrectContentTypeHeader(incomingHeaders))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Message {nativeMessageId} has incorrect CloudEvents JSON Structured Content-Type header and won't be unwrapped");
                }
                metrics.RecordUnwrappingAttempt(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                return null;
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Message {nativeMessageId} has correct CloudEvents JSON Structured Content-Type header and will be unwrapped");
            }
            metrics.RecordUnwrappingAttempt(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);

            JsonDocument? receivedCloudEvent;
            try
            {
                receivedCloudEvent = JsonSerializer.Deserialize<JsonDocument>(body.Span, Options);
            }
            catch (Exception e)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.Warn($"Couldn't deserialize body of the message {nativeMessageId}: {e}");
                }
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw;
            }

            if (receivedCloudEvent == null)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.Warn($"Deserialized unexpected body of the message {nativeMessageId}");
                }
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw new NotSupportedException("Couldn't deserialize the message into a cloud event");
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Message {nativeMessageId} has been deserialized correctly");
            }

            Dictionary<string, JsonProperty> caseInsensitiveProperties = ToCaseInsensitiveDictionary(receivedCloudEvent);
            ThrowIfInvalidCloudEventAndRecordMetrics(nativeMessageId, caseInsensitiveProperties, metrics);
            return caseInsensitiveProperties;
        }

        static bool HasCorrectContentTypeHeader(IDictionary<string, string> incomingHeaders) =>
            incomingHeaders.TryGetValue(Headers.ContentType, out var value) &&
            (value == CloudEventJsonStructuredConstants.SupportedContentType || value.Contains(CloudEventJsonStructuredConstants.SupportedContentType));

        static void ThrowIfInvalidCloudEventAndRecordMetrics(string nativeMessageId,
            Dictionary<string, JsonProperty> receivedCloudEvent, CloudEventsMetrics metrics)
        {
            foreach (var property in RequiredProperties)
            {
                if (!receivedCloudEvent.TryGetValue(property, out _))
                {
                    if (Log.IsWarnEnabled)
                    {
                        Log.Warn($"Message {nativeMessageId} lacks required {property} property");
                    }
                    metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                    throw new NotSupportedException($"Message {nativeMessageId} lacks {property} property");
                }
            }

            if (!receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataBase64Property, out _) &&
                !receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataProperty, out _))
            {
                if (Log.IsWarnEnabled)
                {
                    Log.Warn($"Message {nativeMessageId} lacks both {CloudEventJsonStructuredConstants.DataProperty} and {CloudEventJsonStructuredConstants.DataBase64Property} property");
                }
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw new NotSupportedException($"Message {nativeMessageId} lacks both {CloudEventJsonStructuredConstants.DataProperty} and {CloudEventJsonStructuredConstants.DataBase64Property} property");
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Message {nativeMessageId} has all the required fields");
            }
            metrics.RecordValidMessage(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);

            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.VersionProperty, out var version))
            {
                var versionValue = version.Value.GetString();

                if (versionValue != CloudEventJsonStructuredConstants.SupportedVersion)
                {
                    if (Log.IsWarnEnabled)
                    {
                        Log.Warn($"Unexpected CloudEvent version property value {versionValue} for message {nativeMessageId}");
                    }
                    metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT, versionValue);
                }
                else
                {
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"Message {nativeMessageId} has correct version field");
                    }
                    metrics.RecordUnexpectedVersion(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT, CloudEventJsonStructuredConstants.SupportedVersion);
                }
            }
            else
            {
                if (Log.IsWarnEnabled)
                {
                    Log.Warn($"CloudEvent version property is missing for message id {nativeMessageId}");
                }
                metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT, null);
            }
        }
    }

    static class PermissiveHandler
    {
        internal static Dictionary<string, JsonProperty>? DeserializeOrThrow(string nativeMessageId,
            ReadOnlyMemory<byte> body, CloudEventsMetrics metrics)
        {
            metrics.RecordUnwrappingAttempt(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE);

            JsonDocument? receivedCloudEvent;
            try
            {
                receivedCloudEvent = JsonSerializer.Deserialize<JsonDocument>(body.Span, Options);
            }
            catch (Exception e)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Couldn't deserialize body of the message {nativeMessageId}: {e}");
                }

                return null;
            }

            if (receivedCloudEvent == null)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Deserialized unexpected body of the message {nativeMessageId}");
                }
                return null;
            }

            Dictionary<string, JsonProperty> caseInsensitiveProperties = ToCaseInsensitiveDictionary(receivedCloudEvent);

            if (!caseInsensitiveProperties.TryGetValue(CloudEventJsonStructuredConstants.TypeProperty, out _))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"No data field for the message {nativeMessageId}");
                }
                return null;
            }

            RecordMetrics(nativeMessageId, caseInsensitiveProperties, metrics);

            return caseInsensitiveProperties;
        }

        static void RecordMetrics(string nativeMessageId, Dictionary<string, JsonProperty> receivedCloudEvent,
            CloudEventsMetrics metrics)
        {
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Received correct payload for the message {nativeMessageId}");
            }
            metrics.RecordValidMessage(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE);

            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.VersionProperty, out var version))
            {
                var versionValue = version.Value.GetString();

                if (versionValue != CloudEventJsonStructuredConstants.SupportedVersion)
                {
                    metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE, versionValue);

                    if (Log.IsWarnEnabled)
                    {
                        Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}",
                            versionValue, receivedCloudEvent[CloudEventJsonStructuredConstants.IdProperty].Value.GetString());
                    }
                }
                else
                {
                    metrics.RecordUnexpectedVersion(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE, CloudEventJsonStructuredConstants.SupportedVersion);
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"Received correct version property value for the message {nativeMessageId}");
                    }
                }
            }
            else
            {
                metrics.RecordUnexpectedVersion(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE, null);
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Missing version property value for the message {nativeMessageId}");
                }
            }
        }
    }
}