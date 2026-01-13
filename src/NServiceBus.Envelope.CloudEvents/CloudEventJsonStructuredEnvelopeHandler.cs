namespace NServiceBus.Envelope.CloudEvents;

using System.Buffers;
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

    public Dictionary<string, string>? UnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders, ReadOnlySpan<byte> incomingBody, ContextBag extensions, IBufferWriter<byte> bodyWriter)
    {
        var isStrict = config.EnvelopeUnwrappers.Find<CloudEventJsonStructuredEnvelopeUnwrapper>().EnvelopeHandlingMode == JsonStructureEnvelopeHandlingMode.Strict;

        Dictionary<string, JsonProperty>? receivedCloudEvent = isStrict
            ? StrictHandler.DeserializeOrThrow(nativeMessageId, incomingHeaders, incomingBody, metrics)
            : PermissiveHandler.DeserializeOrThrow(nativeMessageId, incomingBody, metrics);

        if (receivedCloudEvent == null)
        {
            return null;
        }

        ExtractBody(nativeMessageId, receivedCloudEvent, bodyWriter);
        return ExtractHeaders(nativeMessageId, incomingHeaders, receivedCloudEvent);
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
                Log.DebugFormat("Extracted {0} for {1} field for messageId {2}", headersCopy[kvp.Key], kvp.Key, nativeMessageId);
            }
        }

        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.IdProperty, out var id))
        {
            headersCopy[Headers.MessageId] = id;
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Extracted {0} for {1} field for messageId {2}", headersCopy[Headers.MessageId], CloudEventJsonStructuredConstants.IdProperty, nativeMessageId);
            }
        }

        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.SourceProperty, out var source))
        {
            headersCopy[Headers.ReplyToAddress] = source;
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Extracted {0} for {1} field for messageId {2}", headersCopy[Headers.ReplyToAddress], CloudEventJsonStructuredConstants.SourceProperty, nativeMessageId);
            }
        }

        headersCopy[Headers.EnclosedMessageTypes] = ExtractType(receivedCloudEvent);
        if (Log.IsDebugEnabled)
        {
            Log.DebugFormat("Extracted {0} for {1} field for messageId {2}", headersCopy[Headers.EnclosedMessageTypes], CloudEventJsonStructuredConstants.TypeProperty, nativeMessageId);
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
                Log.DebugFormat("Extracted {0} for {1} field for messageId {2}", headersCopy[Headers.TimeSent], CloudEventJsonStructuredConstants.TimeProperty, nativeMessageId);
            }
        }
        else
        {
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("No time extracted for messageId {0}", nativeMessageId);
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
            Log.DebugFormat("Extracted {0} for {1} field for messageId {2}", headersCopy[Headers.ContentType], CloudEventJsonStructuredConstants.DataContentTypeProperty, nativeMessageId);
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

    static void ExtractBody(string nativeMessageId, Dictionary<string, JsonProperty> receivedCloudEvent, IBufferWriter<byte> bodyWriter)
    {
        if (TryGetHeader(receivedCloudEvent, CloudEventJsonStructuredConstants.DataBase64Property, out var base64Body))
        {
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Extracting inner body from {0} for message {1}", CloudEventJsonStructuredConstants.DataBase64Property, nativeMessageId);
            }
            var bytes = Convert.FromBase64String(base64Body);
            bodyWriter.Write(bytes);
            return;
        }

        if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataProperty, out var data))
        {
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Extracting inner body from {0} for message {1}", CloudEventJsonStructuredConstants.DataProperty, nativeMessageId);
            }

            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataContentTypeProperty,
                    out var property) && !property.Value.GetString()!.EndsWith(CloudEventJsonStructuredConstants.JsonSuffix))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Passing inner body as text for message {0}", nativeMessageId);
                }
                var bytes = Encoding.UTF8.GetBytes(data.Value.GetString()!);
                bodyWriter.Write(bytes);
                return;
            }

            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Passing inner body as JSON for message {0}", nativeMessageId);
            }
            if (data.Value.ValueKind is not JsonValueKind.Undefined and not JsonValueKind.Null)
            {
                var bytes = Encoding.UTF8.GetBytes(data.Value.GetRawText());
                bodyWriter.Write(bytes);
                return;
            }
        }

        if (Log.IsDebugEnabled)
        {
            Log.DebugFormat("Empty inner body for message {0}", nativeMessageId);
        }
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
            ReadOnlySpan<byte> body, CloudEventsMetrics metrics)
        {
            if (!HasCorrectContentTypeHeader(incomingHeaders))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Message {0} has incorrect CloudEvents JSON Structured Content-Type header and won't be unwrapped", nativeMessageId);
                }
                metrics.RecordNotAttemptingToUnwrap(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                return null;
            }

            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Message {0} has correct CloudEvents JSON Structured Content-Type header and will be unwrapped", nativeMessageId);
            }
            metrics.RecordAttemptingToUnwrap(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);

            JsonDocument? receivedCloudEvent;
            try
            {
                receivedCloudEvent = JsonSerializer.Deserialize<JsonDocument>(body, Options);
            }
            catch (Exception e)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Couldn't deserialize body of the message {0}: {1}", nativeMessageId, e);
                }
                metrics.RecordInvalidMessage(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw;
            }

            if (receivedCloudEvent == null)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Deserialized unexpected body of the message {0}", nativeMessageId);
                }
                metrics.RecordInvalidMessage(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw new NotSupportedException("Couldn't deserialize the message into a cloud event");
            }

            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Message {0} has been deserialized correctly", nativeMessageId);
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
                        Log.WarnFormat("Message {0} lacks required {1} property", nativeMessageId, property);
                    }
                    metrics.RecordInvalidMessage(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                    throw new NotSupportedException($"Message {nativeMessageId} lacks {property} property");
                }
            }

            if (!receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataBase64Property, out _) &&
                !receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.DataProperty, out _))
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Message {0} lacks both {1} and {2} property", nativeMessageId, CloudEventJsonStructuredConstants.DataProperty, CloudEventJsonStructuredConstants.DataBase64Property);
                }
                metrics.RecordInvalidMessage(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw new NotSupportedException($"Message {nativeMessageId} lacks both {CloudEventJsonStructuredConstants.DataProperty} and {CloudEventJsonStructuredConstants.DataBase64Property} property");
            }

            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Message {0} has all the required fields", nativeMessageId);
            }
            metrics.RecordValidMessage(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);

            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.VersionProperty, out var version))
            {
                var versionValue = version.Value.GetString();

                if (versionValue != CloudEventJsonStructuredConstants.SupportedVersion)
                {
                    if (Log.IsWarnEnabled)
                    {
                        Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}", versionValue, nativeMessageId);
                    }
                    metrics.RecordUnexpectedVersion(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT, versionValue);
                }
                else
                {
                    if (Log.IsDebugEnabled)
                    {
                        Log.DebugFormat("Message {0} has correct version field", nativeMessageId);
                    }
                    metrics.RecordExpectedVersion(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT, CloudEventJsonStructuredConstants.SupportedVersion);
                }
            }
            else
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("CloudEvent version property is missing for message id {0}", nativeMessageId);
                }
                metrics.RecordUnexpectedVersion(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT, null);
            }
        }
    }

    static class PermissiveHandler
    {
        internal static Dictionary<string, JsonProperty>? DeserializeOrThrow(string nativeMessageId,
            ReadOnlySpan<byte> body, CloudEventsMetrics metrics)
        {
            metrics.RecordAttemptingToUnwrap(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE);

            JsonDocument? receivedCloudEvent;
            try
            {
                receivedCloudEvent = JsonSerializer.Deserialize<JsonDocument>(body, Options);
            }
            catch (Exception e)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Couldn't deserialize body of the message {0}: {1}", nativeMessageId, e);
                }

                return null;
            }

            if (receivedCloudEvent == null)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Deserialized unexpected body of the message {0}", nativeMessageId);
                }
                return null;
            }

            Dictionary<string, JsonProperty> caseInsensitiveProperties = ToCaseInsensitiveDictionary(receivedCloudEvent);

            if (!caseInsensitiveProperties.TryGetValue(CloudEventJsonStructuredConstants.TypeProperty, out _))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("No data field for the message {0}", nativeMessageId);
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
                Log.DebugFormat("Received correct payload for the message {0}", nativeMessageId);
            }
            metrics.RecordValidMessage(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE);

            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.VersionProperty, out var version))
            {
                var versionValue = version.Value.GetString();

                if (versionValue != CloudEventJsonStructuredConstants.SupportedVersion)
                {
                    metrics.RecordUnexpectedVersion(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE, versionValue);

                    if (Log.IsWarnEnabled)
                    {
                        Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}",
                            versionValue, receivedCloudEvent[CloudEventJsonStructuredConstants.IdProperty].Value.GetString());
                    }
                }
                else
                {
                    metrics.RecordExpectedVersion(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE, CloudEventJsonStructuredConstants.SupportedVersion);
                    if (Log.IsDebugEnabled)
                    {
                        Log.DebugFormat("Received correct version property value for the message {0}", nativeMessageId);
                    }
                }
            }
            else
            {
                metrics.RecordExpectedVersion(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE, null);
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Missing version property value for the message {0}", nativeMessageId);
                }
            }
        }
    }
}