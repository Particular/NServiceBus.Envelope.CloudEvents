namespace NServiceBus.Envelope.CloudEvents;

using System.Buffers;
using Extensibility;
using Logging;

class CloudEventAmqpBinaryConstants
{
    internal const string HeaderPrefix = "cloudEvents:";
    internal const string TypeProperty = HeaderPrefix + "type";
    internal const string IdProperty = HeaderPrefix + "id";
    internal const string SourceProperty = HeaderPrefix + "source";
    internal const string TimeProperty = HeaderPrefix + "time";
    internal const string VersionProperty = HeaderPrefix + "specversion";
    internal const string SupportedVersion = "1.0";
    internal const string NullLiteral = "null";

    internal static readonly string[] RequiredHeaders = [IdProperty, SourceProperty, TypeProperty];
}

class CloudEventAmqpBinaryEnvelopeHandler(CloudEventsMetrics metrics, CloudEventsConfiguration config) : IEnvelopeHandler
{
    static readonly ILog Log = LogManager.GetLogger<CloudEventAmqpBinaryEnvelopeHandler>();

    public Dictionary<string, string>? UnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders,
        ReadOnlySpan<byte> incomingBody, ContextBag extensions, IBufferWriter<byte> bodyWriter)
    {
        var caseInsensitiveHeaders = ToCaseInsensitiveDictionary(incomingHeaders);
        if (!IsValidMessage(nativeMessageId, caseInsensitiveHeaders))
        {
            return null;
        }

        metrics.EnvelopeUnwrapped(CloudEventsMetrics.CloudEventTypes.AMQP_BINARY);
        bodyWriter.Write(incomingBody);
        return ExtractHeaders(nativeMessageId, caseInsensitiveHeaders);
    }

    static Dictionary<string, string> ToCaseInsensitiveDictionary(IDictionary<string, string> incomingHeaders) =>
        incomingHeaders
            .ToDictionary(p => p.Key, p => p.Value,
                StringComparer.OrdinalIgnoreCase);

    Dictionary<string, string> ExtractHeaders(string nativeMessageId, IDictionary<string, string> existingHeaders)
    {
        var headersCopy = existingHeaders.ToDictionary(k => k.Key, k => k.Value);

        headersCopy[Headers.MessageId] = ExtractId(existingHeaders);
        if (Log.IsDebugEnabled)
        {
            Log.DebugFormat("Extracted {0} for {1} field for messageId {2}", headersCopy[Headers.MessageId], CloudEventJsonStructuredConstants.IdProperty, nativeMessageId);
        }

        headersCopy[Headers.ReplyToAddress] = ExtractSource(existingHeaders);
        if (Log.IsDebugEnabled)
        {
            Log.DebugFormat("Extracted {0} for {1} field for messageId {2}", headersCopy[Headers.ReplyToAddress], CloudEventJsonStructuredConstants.SourceProperty, nativeMessageId);
        }

        headersCopy[Headers.EnclosedMessageTypes] = ExtractType(existingHeaders);
        if (Log.IsDebugEnabled)
        {
            Log.DebugFormat("Extracted {0} for {1} field for messageId {2}", headersCopy[Headers.EnclosedMessageTypes], CloudEventJsonStructuredConstants.TypeProperty, nativeMessageId);
        }

        if (existingHeaders.TryGetValue(CloudEventAmqpBinaryConstants.TimeProperty, out var time)
            && !string.IsNullOrEmpty(time) && time != CloudEventAmqpBinaryConstants.NullLiteral)
        {
            /*
                 * If what comes in is something similar to "2018-04-05T17:31:00Z", compliant with the CloudEvents spec
                 * and ISO 8601, NServiceBus will not be happy and later in the pipeline there will be a parsing exception
                 */
            headersCopy[Headers.TimeSent] = DateTimeOffsetHelper.ToWireFormattedString(DateTimeOffset.Parse(time));
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

        return headersCopy;
    }

    static string ExtractId(IDictionary<string, string> existingHeaders) => ExtractHeader(existingHeaders, CloudEventAmqpBinaryConstants.IdProperty);

    string ExtractType(IDictionary<string, string> existingHeaders)
    {
        var cloudEventType = ExtractHeader(existingHeaders, CloudEventAmqpBinaryConstants.TypeProperty);
        return config.TypeMappings.TryGetValue(cloudEventType, out var typeMapping)
            ? string.Join(',', typeMapping)
            : cloudEventType;
    }

    static string ExtractSource(IDictionary<string, string> existingHeaders) => ExtractHeader(existingHeaders, CloudEventAmqpBinaryConstants.SourceProperty);

    static string ExtractHeader(IDictionary<string, string> existingHeaders, string property) => existingHeaders[property];

    bool IsValidMessage(string nativeMessageId, IDictionary<string, string> headers)
    {
        if (!HasRequiredHeaders(nativeMessageId, headers))
        {
            return false;
        }

        if (headers.TryGetValue(CloudEventAmqpBinaryConstants.VersionProperty, out var version))
        {
            if (version != CloudEventAmqpBinaryConstants.SupportedVersion)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}", version, nativeMessageId);
                }
                metrics.VersionMismatch(CloudEventsMetrics.CloudEventTypes.AMQP_BINARY, version);
            }
            else
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Correct version field  for message {0}", nativeMessageId);
                }
            }
        }
        else
        {
            if (Log.IsWarnEnabled)
            {
                Log.WarnFormat("CloudEvent version property is missing for message id {0}", nativeMessageId);
            }
            metrics.VersionMismatch(CloudEventsMetrics.CloudEventTypes.AMQP_BINARY, null);
        }

        return true;
    }

    static bool HasRequiredHeaders(string nativeMessageId, IDictionary<string, string> incomingHeaders)
    {
        foreach (var header in CloudEventAmqpBinaryConstants.RequiredHeaders)
        {
            if (!incomingHeaders.ContainsKey(header))
            {
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("Message {0} has no property {1}", nativeMessageId, header);
                }

                return false;
            }
        }

        if (Log.IsDebugEnabled)
        {
            Log.DebugFormat("Message {0} has all required properties", nativeMessageId);
        }

        return true;
    }
}
