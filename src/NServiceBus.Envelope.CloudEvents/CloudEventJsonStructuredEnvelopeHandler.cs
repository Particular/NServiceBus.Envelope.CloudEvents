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

class CloudEventPropertyValue
{
    public JsonValueKind ValueKind { get; init; }
    public string? StringValue { get; init; }
    public ReadOnlyMemory<byte> RawJsonBytes { get; init; }

    public string GetString() => StringValue ?? throw new InvalidOperationException("Value is not a string");
    public string GetRawText() => Encoding.UTF8.GetString(RawJsonBytes.Span);
}

class CloudEventProperties
{
    readonly Dictionary<string, CloudEventPropertyValue> properties = new(StringComparer.OrdinalIgnoreCase);

    public void Add(string name, CloudEventPropertyValue value) => properties[name] = value;

    public bool TryGetValue(string key, [MaybeNullWhen(false)] out CloudEventPropertyValue value) =>
        properties.TryGetValue(key, out value);

    public bool ContainsKey(string key) => properties.ContainsKey(key);

    public CloudEventPropertyValue this[string key] => properties[key];

    public IEnumerable<KeyValuePair<string, CloudEventPropertyValue>> Properties => properties;
}

class CloudEventJsonStructuredEnvelopeHandler(CloudEventsMetrics metrics, CloudEventsConfiguration config) : IEnvelopeHandler
{
    static readonly ILog Log = LogManager.GetLogger<CloudEventJsonStructuredEnvelopeHandler>();

    public Dictionary<string, string>? UnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders, ReadOnlySpan<byte> incomingBody, ContextBag extensions, IBufferWriter<byte> bodyWriter)
    {
        var isStrict = config.EnvelopeUnwrappers.Find<CloudEventJsonStructuredEnvelopeUnwrapper>().EnvelopeHandlingMode == JsonStructureEnvelopeHandlingMode.Strict;

        CloudEventProperties? receivedCloudEvent = isStrict
            ? StrictHandler.DeserializeOrThrow(nativeMessageId, incomingHeaders, incomingBody, metrics)
            : PermissiveHandler.DeserializeOrThrow(nativeMessageId, incomingBody, metrics);

        if (receivedCloudEvent == null)
        {
            return null;
        }

        ExtractBody(nativeMessageId, receivedCloudEvent, bodyWriter);
        return ExtractHeaders(nativeMessageId, incomingHeaders, receivedCloudEvent);
    }

    static CloudEventProperties? ParseCloudEventJson(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json);

        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            return null;
        }

        var properties = new CloudEventProperties();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                break;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                continue;
            }

            var propertyName = reader.GetString();
            if (propertyName == null)
            {
                continue;
            }

            // Normalize to lowercase for case-insensitive matching
            propertyName = propertyName.ToLowerInvariant();

            reader.Read();

            var valueKind = reader.TokenType switch
            {
                JsonTokenType.String => JsonValueKind.String,
                JsonTokenType.Number => JsonValueKind.Number,
                JsonTokenType.True or JsonTokenType.False => JsonValueKind.True,
                JsonTokenType.Null => JsonValueKind.Null,
                JsonTokenType.StartObject => JsonValueKind.Object,
                JsonTokenType.StartArray => JsonValueKind.Array,
                JsonTokenType.None => JsonValueKind.Undefined,
                JsonTokenType.Comment => JsonValueKind.Undefined,
                JsonTokenType.EndObject => JsonValueKind.Undefined,
                JsonTokenType.EndArray => JsonValueKind.Undefined,
                JsonTokenType.PropertyName => JsonValueKind.Undefined,
                _ => JsonValueKind.Undefined
            };

            CloudEventPropertyValue value = valueKind switch
            {
                JsonValueKind.String => new CloudEventPropertyValue
                {
                    ValueKind = JsonValueKind.String,
                    StringValue = reader.GetString()
                },
                JsonValueKind.Object or JsonValueKind.Array => CaptureComplexValue(ref reader, json, valueKind),
                JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False or JsonValueKind.Null or JsonValueKind.Undefined => CapturePrimitiveValue(ref reader, valueKind),
                _ => CapturePrimitiveValue(ref reader, valueKind)
            };

            properties.Add(propertyName, value);
        }

        return properties;
    }

    static CloudEventPropertyValue CaptureComplexValue(ref Utf8JsonReader reader, ReadOnlySpan<byte> json, JsonValueKind valueKind)
    {
        // Capture the raw JSON for complex types
        var startPosition = reader.TokenStartIndex;
        var depth = reader.CurrentDepth;

        // Skip the entire object/array
        while (reader.Read() && reader.CurrentDepth > depth)
        {
        }

        var length = (int)(reader.TokenStartIndex + reader.ValueSpan.Length - startPosition);
        var rawJson = json.Slice((int)startPosition, length);

        return new CloudEventPropertyValue
        {
            ValueKind = valueKind,
            RawJsonBytes = rawJson.ToArray()
        };
    }

    static CloudEventPropertyValue CapturePrimitiveValue(ref Utf8JsonReader reader, JsonValueKind valueKind)
    {
        // For primitive types, capture as both string and raw JSON
        var rawJson = reader.ValueSpan;
        return new CloudEventPropertyValue
        {
            ValueKind = valueKind,
            StringValue = valueKind == JsonValueKind.Number ? reader.GetDouble().ToString() : reader.GetString(),
            RawJsonBytes = rawJson.ToArray()
        };
    }

    Dictionary<string, string> ExtractHeaders(string nativeMessageId, IDictionary<string, string> existingHeaders,
        CloudEventProperties receivedCloudEvent)
    {
        var headersCopy = existingHeaders.ToDictionary(k => k.Key, k => k.Value);

        foreach (var kvp in receivedCloudEvent.Properties)
        {
            if (
                CloudEventJsonStructuredConstants.HeadersToIgnore.Contains(kvp.Key)
                || kvp.Value.ValueKind == JsonValueKind.Undefined
                || kvp.Value.ValueKind == JsonValueKind.Null
            )
            {
                continue;
            }

            headersCopy[kvp.Key] = kvp.Value.ValueKind == JsonValueKind.String
                ? kvp.Value.GetString()
                : kvp.Value.GetRawText();

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

    string ExtractType(CloudEventProperties receivedCloudEvent)
    {
        var cloudEventType = receivedCloudEvent[CloudEventJsonStructuredConstants.TypeProperty].GetString();
        return config.TypeMappings.TryGetValue(cloudEventType, out var typeMapping)
            ? string.Join(',', typeMapping)
            : cloudEventType;
    }

    static void ExtractBody(string nativeMessageId, CloudEventProperties receivedCloudEvent, IBufferWriter<byte> bodyWriter)
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
                    out var property) && !property.GetString().EndsWith(CloudEventJsonStructuredConstants.JsonSuffix))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Passing inner body as text for message {0}", nativeMessageId);
                }
                var bytes = Encoding.UTF8.GetBytes(data.GetString());
                bodyWriter.Write(bytes);
                return;
            }

            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Passing inner body as JSON for message {0}", nativeMessageId);
            }
            if (data.ValueKind is not JsonValueKind.Undefined and not JsonValueKind.Null)
            {
                bodyWriter.Write(data.RawJsonBytes.Span);
                return;
            }
        }

        if (Log.IsDebugEnabled)
        {
            Log.DebugFormat("Empty inner body for message {0}", nativeMessageId);
        }
    }

    static bool TryGetHeader(CloudEventProperties receivedCloudEvent, string header, [MaybeNullWhen(false)] out string result)
    {
        if (receivedCloudEvent.TryGetValue(header, out var value)
            && value.ValueKind != JsonValueKind.Undefined
            && value.ValueKind != JsonValueKind.Null)
        {
            result = value.GetString();
            return true;
        }

        result = null;
        return false;
    }

    static class StrictHandler
    {
        static readonly string[] RequiredProperties = [
            CloudEventJsonStructuredConstants.IdProperty,
            CloudEventJsonStructuredConstants.SourceProperty,
            CloudEventJsonStructuredConstants.TypeProperty
        ];

        internal static CloudEventProperties? DeserializeOrThrow(string nativeMessageId,
            IDictionary<string, string> incomingHeaders,
            ReadOnlySpan<byte> body, CloudEventsMetrics metrics)
        {
            if (!HasCorrectContentTypeHeader(incomingHeaders))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Message {0} has incorrect CloudEvents JSON Structured Content-Type header and won't be unwrapped", nativeMessageId);
                }
                return null;
            }

            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Message {0} has correct CloudEvents JSON Structured Content-Type header and will be unwrapped", nativeMessageId);
            }
            metrics.EnvelopeUnwrapped(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);

            CloudEventProperties? receivedCloudEvent;
            try
            {
                receivedCloudEvent = ParseCloudEventJson(body);
            }
            catch (Exception e)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Couldn't deserialize body of the message {0}: {1}", nativeMessageId, e);
                }
                metrics.MessageInvalid(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw;
            }

            if (receivedCloudEvent == null)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Deserialized unexpected body of the message {0}", nativeMessageId);
                }
                metrics.MessageInvalid(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw new NotSupportedException("Couldn't deserialize the message into a cloud event");
            }

            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Message {0} has been deserialized correctly", nativeMessageId);
            }

            ThrowIfInvalidCloudEventAndRecordMetrics(nativeMessageId, receivedCloudEvent, metrics);
            return receivedCloudEvent;
        }

        static bool HasCorrectContentTypeHeader(IDictionary<string, string> incomingHeaders) =>
            incomingHeaders.TryGetValue(Headers.ContentType, out var value) &&
            (value == CloudEventJsonStructuredConstants.SupportedContentType || value.Contains(CloudEventJsonStructuredConstants.SupportedContentType));

        static void ThrowIfInvalidCloudEventAndRecordMetrics(string nativeMessageId,
            CloudEventProperties receivedCloudEvent, CloudEventsMetrics metrics)
        {
            foreach (var property in RequiredProperties)
            {
                if (!receivedCloudEvent.ContainsKey(property))
                {
                    if (Log.IsWarnEnabled)
                    {
                        Log.WarnFormat("Message {0} lacks required {1} property", nativeMessageId, property);
                    }
                    metrics.MessageInvalid(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                    throw new NotSupportedException($"Message {nativeMessageId} lacks {property} property");
                }
            }

            if (!receivedCloudEvent.ContainsKey(CloudEventJsonStructuredConstants.DataBase64Property) &&
                !receivedCloudEvent.ContainsKey(CloudEventJsonStructuredConstants.DataProperty))
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Message {0} lacks both {1} and {2} property", nativeMessageId, CloudEventJsonStructuredConstants.DataProperty, CloudEventJsonStructuredConstants.DataBase64Property);
                }
                metrics.MessageInvalid(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw new NotSupportedException($"Message {nativeMessageId} lacks both {CloudEventJsonStructuredConstants.DataProperty} and {CloudEventJsonStructuredConstants.DataBase64Property} property");
            }

            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Message {0} has all the required fields", nativeMessageId);
            }

            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.VersionProperty, out var version))
            {
                var versionValue = version.GetString();

                if (versionValue != CloudEventJsonStructuredConstants.SupportedVersion)
                {
                    if (Log.IsWarnEnabled)
                    {
                        Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}", versionValue, nativeMessageId);
                    }
                    metrics.VersionMismatch(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT, versionValue);
                }
                else
                {
                    if (Log.IsDebugEnabled)
                    {
                        Log.DebugFormat("Message {0} has correct version field", nativeMessageId);
                    }
                }
            }
            else
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("CloudEvent version property is missing for message id {0}", nativeMessageId);
                }
                metrics.VersionMismatch(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT, null);
            }
        }
    }

    static class PermissiveHandler
    {
        internal static CloudEventProperties? DeserializeOrThrow(string nativeMessageId,
            ReadOnlySpan<byte> body, CloudEventsMetrics metrics)
        {
            metrics.EnvelopeUnwrapped(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE);

            CloudEventProperties? receivedCloudEvent;
            try
            {
                receivedCloudEvent = ParseCloudEventJson(body);
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

            if (!receivedCloudEvent.ContainsKey(CloudEventJsonStructuredConstants.TypeProperty))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("No type field for the message {0}", nativeMessageId);
                }
                return null;
            }

            RecordMetrics(nativeMessageId, receivedCloudEvent, metrics);

            return receivedCloudEvent;
        }

        static void RecordMetrics(string nativeMessageId, CloudEventProperties receivedCloudEvent,
            CloudEventsMetrics metrics)
        {
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("Received correct payload for the message {0}", nativeMessageId);
            }

            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.VersionProperty, out var version))
            {
                var versionValue = version.GetString();

                if (versionValue != CloudEventJsonStructuredConstants.SupportedVersion)
                {
                    metrics.VersionMismatch(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE, versionValue);

                    if (Log.IsWarnEnabled)
                    {
                        Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}",
                            versionValue, receivedCloudEvent[CloudEventJsonStructuredConstants.IdProperty].GetString());
                    }
                }
                else
                {
                    if (Log.IsDebugEnabled)
                    {
                        Log.DebugFormat("Received correct version property value for the message {0}", nativeMessageId);
                    }
                }
            }
            else
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Missing version property value for the message {0}", nativeMessageId);
                }
            }
        }
    }
}