namespace NServiceBus.Envelope.CloudEvents;

using System;
using System.Collections.Generic;
using System.Linq;
using Extensibility;
using Logging;

class CloudEventAmqpBinaryEnvelopeHandler(CloudEventsMetrics metrics) : IEnvelopeHandler
{
    static readonly ILog Log = LogManager.GetLogger<CloudEventAmqpBinaryEnvelopeHandler>();

    const string HEADER_PREFIX = "cloudEvents:";
    const string TYPE_PROPERTY = HEADER_PREFIX + "type";
    const string ID_PROPERTY = HEADER_PREFIX + "id";
    const string SOURCE_PROPERTY = HEADER_PREFIX + "source";
    const string TIME_PROPERTY = HEADER_PREFIX + "time";
    const string VERSION_PROPERTY = HEADER_PREFIX + "specversion";
    const string SUPPORTED_VERSION = "1.0";

    static readonly string[] REQUIRED_HEADERS = [ID_PROPERTY, SOURCE_PROPERTY, TYPE_PROPERTY];

    public (Dictionary<string, string> headers, ReadOnlyMemory<byte> body) UnwrapEnvelope(
        string nativeMessageId, IDictionary<string, string> incomingHeaders,
        ContextBag extensions, ReadOnlyMemory<byte> incomingBody)
    {
        ThrowIfInvalidMessage(incomingHeaders);
        var headers = ExtractHeaders(incomingHeaders);
        return (headers, incomingBody);
    }

    static Dictionary<string, string> ExtractHeaders(IDictionary<string, string> existingHeaders)
    {
        var headersCopy = existingHeaders.ToDictionary(k => k.Key, k => k.Value);

        headersCopy[Headers.MessageId] = ExtractId(existingHeaders);
        headersCopy[Headers.ReplyToAddress] = ExtractSource(existingHeaders);
        if (existingHeaders.TryGetValue(TIME_PROPERTY, out var time))
        {
            headersCopy[Headers.TimeSent] = time;
        }

        return headersCopy;
    }

    static string ExtractId(IDictionary<string, string> existingHeaders) => ExtractHeader(existingHeaders, ID_PROPERTY);

    static string ExtractSource(IDictionary<string, string> existingHeaders) => ExtractHeader(existingHeaders, SOURCE_PROPERTY);

    static string ExtractHeader(IDictionary<string, string> existingHeaders, string property) => existingHeaders[property];

    void ThrowIfInvalidMessage(IDictionary<string, string> headers)
    {
        if (!HasRequiredHeaders(headers))
        {
            metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.AMQP_BINARY);
            throw new NotSupportedException(
                $"Missing headers: {string.Join(",", REQUIRED_HEADERS.Where(h => !headers.ContainsKey(h)))}");
        }

        metrics.RecordValidMessage(true, CloudEventsMetrics.CloudEventTypes.AMQP_BINARY);

        if (headers.TryGetValue(VERSION_PROPERTY, out var version))
        {
            if (version != SUPPORTED_VERSION)
            {
                metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.AMQP_BINARY, version);

                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}",
                        version, headers[ID_PROPERTY]);
                }
            }
            else
            {
                metrics.RecordUnexpectedVersion(true, CloudEventsMetrics.CloudEventTypes.AMQP_BINARY, SUPPORTED_VERSION);
            }
        }
        else
        {
            metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.AMQP_BINARY, null);

            if (Log.IsWarnEnabled)
            {
                Log.WarnFormat("CloudEvent version property is missing for message id {0}", headers[ID_PROPERTY]);
            }
        }
    }

    static bool HasRequiredHeaders(IDictionary<string, string> incomingHeaders) => REQUIRED_HEADERS.All(incomingHeaders.ContainsKey);

    public bool CanUnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders, ContextBag extensions, ReadOnlyMemory<byte> incomingBody) =>
        REQUIRED_HEADERS.All(incomingHeaders.ContainsKey);
}
