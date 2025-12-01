namespace NServiceBus.Envelope.CloudEvents;

using System;
using System.Collections.Generic;
using System.Linq;
using Extensibility;
using Logging;

class CloudEventAmqpBinaryEnvelopeHandler : IEnvelopeHandler
{
    static readonly ILog Log = LogManager.GetLogger<CloudEventAmqpBinaryEnvelopeHandler>();

    const string HEADER_PREFIX = "cloudEvents:";
    const string TYPE_PROPERTY = HEADER_PREFIX + "type";
    const string ID_PROPERTY = HEADER_PREFIX + "id";
    const string SOURCE_PROPERTY = HEADER_PREFIX + "source";
    const string TIME_PROPERTY = HEADER_PREFIX + "time";
    const string VERSION_PROPERTY = "specversion";
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

    static void ThrowIfInvalidMessage(IDictionary<string, string> headers)
    {
        if (!HasRequiredHeaders(headers))
        {
            throw new NotSupportedException(
                $"Missing headers: {string.Join(",", REQUIRED_HEADERS.Where(h => !headers.ContainsKey(h)))}");
        }

        if (headers.TryGetValue(VERSION_PROPERTY, out var version))
        {
            if (version != SUPPORTED_VERSION)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}",
                        version, headers[ID_PROPERTY]);
                }
            }
        }
        else
        {
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
