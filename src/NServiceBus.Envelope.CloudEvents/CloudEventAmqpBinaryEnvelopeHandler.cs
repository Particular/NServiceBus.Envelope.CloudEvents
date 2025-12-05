namespace NServiceBus.Envelope.CloudEvents;

using System;
using System.Collections.Generic;
using System.Linq;
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

    public (Dictionary<string, string> headers, ReadOnlyMemory<byte> body) UnwrapEnvelope(
        string nativeMessageId, IDictionary<string, string> incomingHeaders,
        ContextBag extensions, ReadOnlyMemory<byte> incomingBody)
    {
        var caseInsensitiveHeaders = ToCaseInsensitiveDictionary(incomingHeaders);
        ThrowIfInvalidMessage(caseInsensitiveHeaders);
        var headers = ExtractHeaders(caseInsensitiveHeaders);
        return (headers, incomingBody);
    }

    static Dictionary<string, string> ToCaseInsensitiveDictionary(IDictionary<string, string> incomingHeaders) =>
        incomingHeaders
            .ToDictionary(p => p.Key, p => p.Value,
                StringComparer.OrdinalIgnoreCase);

    Dictionary<string, string> ExtractHeaders(IDictionary<string, string> existingHeaders)
    {
        var headersCopy = existingHeaders.ToDictionary(k => k.Key, k => k.Value);

        headersCopy[Headers.MessageId] = ExtractId(existingHeaders);
        headersCopy[Headers.ReplyToAddress] = ExtractSource(existingHeaders);
        headersCopy[Headers.EnclosedMessageTypes] = ExtractType(existingHeaders);
        if (existingHeaders.TryGetValue(CloudEventAmqpBinaryConstants.TimeProperty, out var time))
        {
            if (!string.IsNullOrEmpty(time) && time != CloudEventHttpBinaryEnvelopeHandlerConstants.NullLiteral)
            {
                /*
                 * If what comes in is something similar to "2018-04-05T17:31:00Z", compliant with the CloudEvents spec
                 * and ISO 8601, NServiceBus will not be happy and later in the pipeline there will be a parsing exception
                 */
                headersCopy[Headers.TimeSent] = DateTimeOffsetHelper.ToWireFormattedString(DateTimeOffset.Parse(time));
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

    void ThrowIfInvalidMessage(IDictionary<string, string> headers)
    {
        if (!HasRequiredHeaders(headers))
        {
            metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.AMQP_BINARY);
            throw new NotSupportedException(
                $"Missing headers: {string.Join(",", CloudEventAmqpBinaryConstants.RequiredHeaders.Where(h => !headers.ContainsKey(h)))}");
        }

        metrics.RecordValidMessage(true, CloudEventsMetrics.CloudEventTypes.AMQP_BINARY);

        if (headers.TryGetValue(CloudEventAmqpBinaryConstants.VersionProperty, out var version))
        {
            if (version != CloudEventAmqpBinaryConstants.SupportedVersion)
            {
                metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.AMQP_BINARY, version);

                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}",
                        version, headers[CloudEventAmqpBinaryConstants.IdProperty]);
                }
            }
            else
            {
                metrics.RecordUnexpectedVersion(true, CloudEventsMetrics.CloudEventTypes.AMQP_BINARY, CloudEventAmqpBinaryConstants.SupportedVersion);
            }
        }
        else
        {
            metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.AMQP_BINARY, null);

            if (Log.IsWarnEnabled)
            {
                Log.WarnFormat("CloudEvent version property is missing for message id {0}", headers[CloudEventAmqpBinaryConstants.IdProperty]);
            }
        }
    }

    static bool HasRequiredHeaders(IDictionary<string, string> incomingHeaders) => CloudEventAmqpBinaryConstants.RequiredHeaders.All(incomingHeaders.ContainsKey);

    public bool CanUnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders, ContextBag extensions, ReadOnlyMemory<byte> incomingBody) => CloudEventAmqpBinaryConstants.RequiredHeaders.All(incomingHeaders.ContainsKey);
}
