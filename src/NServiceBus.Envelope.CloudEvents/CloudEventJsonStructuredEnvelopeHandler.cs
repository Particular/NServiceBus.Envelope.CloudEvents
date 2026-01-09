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

    public (Dictionary<string, string> headers, ReadOnlyMemory<byte> body)? UnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders, ContextBag extensions, ReadOnlyMemory<byte> incomingBody)
    {
        var isStrict = config.EnvelopeUnwrappers.Find<CloudEventJsonStructuredEnvelopeUnwrapper>().EnvelopeHandlingMode == JsonStructureEnvelopeHandlingMode.Strict;

        Dictionary<string, JsonElement>? receivedCloudEvent = isStrict
            ? StrictHandler.DeserializeOrThrow(nativeMessageId, incomingHeaders, incomingBody, metrics)
            : PermissiveHandler.DeserializeOrThrow(nativeMessageId, incomingBody, metrics);

        return receivedCloudEvent == null
            ? null
            : (ExtractHeaders(nativeMessageId, incomingHeaders, receivedCloudEvent), ExtractBody(nativeMessageId, receivedCloudEvent));
    }

    Dictionary<string, string> ExtractHeaders(string nativeMessageId, IDictionary<string, string> existingHeaders,
        Dictionary<string, JsonElement> receivedCloudEvent)
    {
        var headersCopy = existingHeaders.ToDictionary(k => k.Key, k => k.Value);

        foreach (var kvp in receivedCloudEvent)
        {
            var normalizedKey = kvp.Key.ToLowerInvariant();
            if (
                CloudEventJsonStructuredConstants.HeadersToIgnore.Contains(normalizedKey)
                || kvp.Value.ValueKind == JsonValueKind.Undefined
                || kvp.Value.ValueKind == JsonValueKind.Null
            )
            {
                continue;
            }

            headersCopy[normalizedKey] = kvp.Value.ValueKind == JsonValueKind.String
                ? kvp.Value.GetString()!
                : kvp.Value.GetRawText();

            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Extracted {headersCopy[normalizedKey]} for {normalizedKey} field for messageId {nativeMessageId}");
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

    string ExtractType(Dictionary<string, JsonElement> receivedCloudEvent)
    {
        var cloudEventType = receivedCloudEvent[CloudEventJsonStructuredConstants.TypeProperty].GetString()!;
        return config.TypeMappings.TryGetValue(cloudEventType, out var typeMapping)
            ? string.Join(',', typeMapping)
            : cloudEventType;
    }

    static ReadOnlyMemory<byte> ExtractBody(string nativeMessageId, Dictionary<string, JsonElement> receivedCloudEvent)
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
                    out var property) && !property.GetString()!.EndsWith(CloudEventJsonStructuredConstants.JsonSuffix))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Passing inner body as text for message {nativeMessageId}");
                }
                return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(
                    data.GetString()!));
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Passing inner body as JSON for message {nativeMessageId}");
            }
            if (data.ValueKind is not JsonValueKind.Undefined and not JsonValueKind.Null)
            {
                return new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(
                    data.GetRawText()));
            }
        }

        if (Log.IsDebugEnabled)
        {
            Log.Debug($"Empty inner body for message {nativeMessageId}");
        }
        return new ReadOnlyMemory<byte>();
    }

    static bool TryGetHeader(Dictionary<string, JsonElement> receivedCloudEvent, string header, [MaybeNullWhen(false)] out string result)
    {
        if (receivedCloudEvent.TryGetValue(header, out var value)
            && value.ValueKind != JsonValueKind.Undefined
            && value.ValueKind != JsonValueKind.Null)
        {
            result = value.GetString()!;
            return true;
        }

        result = null;
        return false;
    }

    static class StrictHandler
    {
        internal static Dictionary<string, JsonElement>? DeserializeOrThrow(string nativeMessageId,
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

            Dictionary<string, JsonElement>? receivedCloudEvent;
            bool hasData;
            bool isValid;

            try
            {
                receivedCloudEvent = ParseAndValidate(body.Span, out hasData, out isValid);
            }
            catch (Exception e)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.Warn($"Couldn't parse JSON body of the message {nativeMessageId}: {e}");
                }
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw;
            }

            if (receivedCloudEvent == null)
            {
                var message = $"Couldn't deserialize body of the message {nativeMessageId}";
                if (Log.IsWarnEnabled)
                {
                    Log.Warn(message);
                }
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw new NotSupportedException(message);
            }

            if (!isValid)
            {
                var message = $"Message {nativeMessageId} is missing one or more required properties";
                if (Log.IsWarnEnabled)
                {
                    Log.Warn(message);
                }
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw new NotSupportedException(message);
            }

            if (!hasData)
            {
                var message = $"Message {nativeMessageId} lacks both {CloudEventJsonStructuredConstants.DataProperty} and {CloudEventJsonStructuredConstants.DataBase64Property} property";
                if (Log.IsWarnEnabled)
                {
                    Log.Warn(message);
                }
                metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);
                throw new NotSupportedException(message);
            }

            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Message {nativeMessageId} has been deserialized correctly and has all required fields");
            }

            RecordMetricsForValidMessage(nativeMessageId, receivedCloudEvent, metrics);
            return receivedCloudEvent;
        }

        static Dictionary<string, JsonElement>? ParseAndValidate(ReadOnlySpan<byte> jsonBytes, out bool hasData, out bool isValid)
        {
            hasData = false;
            isValid = false;

            var reader = new Utf8JsonReader(jsonBytes);

            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            {
                return null;
            }

            // Pre-allocate for typical CloudEvent size (7-10 properties)
            var properties = new Dictionary<string, JsonElement>(8, StringComparer.OrdinalIgnoreCase);

            // Use bit flags to track required properties: id=1, source=2, type=4
            var foundRequired = 0;

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

                var propertyName = reader.GetString()!;

                // Track required properties as we encounter them
                if (propertyName.Equals(CloudEventJsonStructuredConstants.IdProperty, StringComparison.OrdinalIgnoreCase))
                {
                    foundRequired |= 1;
                }
                else if (propertyName.Equals(CloudEventJsonStructuredConstants.SourceProperty, StringComparison.OrdinalIgnoreCase))
                {
                    foundRequired |= 2;
                }
                else if (propertyName.Equals(CloudEventJsonStructuredConstants.TypeProperty, StringComparison.OrdinalIgnoreCase))
                {
                    foundRequired |= 4;
                }

                if (propertyName.Equals(CloudEventJsonStructuredConstants.DataProperty, StringComparison.OrdinalIgnoreCase) || propertyName.Equals(CloudEventJsonStructuredConstants.DataBase64Property, StringComparison.OrdinalIgnoreCase))
                {
                    hasData = true;
                }

                // Parse the value
                _ = reader.Read();
                var element = JsonElement.ParseValue(ref reader);
                properties[propertyName] = element;
            }

            // Check if all required properties were found (id=1, source=2, type=4 => total=7)
            isValid = foundRequired == 7;

            return properties;
        }

        static bool HasCorrectContentTypeHeader(IDictionary<string, string> incomingHeaders) =>
            incomingHeaders.TryGetValue(Headers.ContentType, out var value) &&
            (value == CloudEventJsonStructuredConstants.SupportedContentType || value.Contains(CloudEventJsonStructuredConstants.SupportedContentType));

        static void RecordMetricsForValidMessage(string nativeMessageId,
            Dictionary<string, JsonElement> receivedCloudEvent, CloudEventsMetrics metrics)
        {
            metrics.RecordValidMessage(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_STRICT);

            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.VersionProperty, out var version))
            {
                var versionValue = version.GetString();

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
        internal static Dictionary<string, JsonElement>? DeserializeOrThrow(string nativeMessageId,
            ReadOnlyMemory<byte> body, CloudEventsMetrics metrics)
        {
            metrics.RecordUnwrappingAttempt(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE);

            Dictionary<string, JsonElement>? receivedCloudEvent;

            try
            {
                receivedCloudEvent = ParseAndValidate(body.Span);
            }
            catch (Exception e)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"Couldn't parse JSON body of the message {nativeMessageId}: {e}");
                }
                return null;
            }

            if (receivedCloudEvent == null)
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"No type field for the message {nativeMessageId}");
                }
                return null;
            }

            RecordMetrics(nativeMessageId, receivedCloudEvent, metrics);

            return receivedCloudEvent;
        }

        static Dictionary<string, JsonElement>? ParseAndValidate(ReadOnlySpan<byte> jsonBytes)
        {
            var reader = new Utf8JsonReader(jsonBytes);

            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            {
                return null;
            }

            bool hasTypeProperty = false;
            // 8 properties on average per CloudEvent?
            var properties = new Dictionary<string, JsonElement>(8, StringComparer.OrdinalIgnoreCase);

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

                var propertyName = reader.GetString()!;
                if (propertyName.Equals(CloudEventJsonStructuredConstants.TypeProperty, StringComparison.OrdinalIgnoreCase))
                {
                    hasTypeProperty = true;
                }

                _ = reader.Read();
                var element = JsonElement.ParseValue(ref reader);
                properties[propertyName] = element;
            }

            return hasTypeProperty ? properties : null;
        }

        static void RecordMetrics(string nativeMessageId, Dictionary<string, JsonElement> receivedCloudEvent,
            CloudEventsMetrics metrics)
        {
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Received correct payload for the message {nativeMessageId}");
            }
            metrics.RecordValidMessage(true, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE);

            if (receivedCloudEvent.TryGetValue(CloudEventJsonStructuredConstants.VersionProperty, out var version))
            {
                var versionValue = version.GetString();

                if (versionValue != CloudEventJsonStructuredConstants.SupportedVersion)
                {
                    metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED_PERMISSIVE, versionValue);

                    if (Log.IsWarnEnabled)
                    {
                        Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}",
                            versionValue, receivedCloudEvent[CloudEventJsonStructuredConstants.IdProperty].GetString());
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