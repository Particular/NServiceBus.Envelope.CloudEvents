namespace NServiceBus.Envelope.CloudEvents;

using System;
using System.Collections.Generic;
using System.Linq;
using Extensibility;
using Logging;

class CloudEventHttpBinaryEnvelopeHandlerConstants
{
    internal const string HeaderPrefix = "ce-";
    internal const string TypeProperty = HeaderPrefix + "type";
    internal const string IdProperty = HeaderPrefix + "id";
    internal const string SourceProperty = HeaderPrefix + "source";
    internal const string TimeProperty = HeaderPrefix + "time";
    internal const string VersionProperty = HeaderPrefix + "specversion";
    internal const string SupportedVersion = "1.0";

    internal static readonly string[] RequiredHeaders = [IdProperty, SourceProperty, TypeProperty];
}

class CloudEventHttpBinaryEnvelopeHandler(CloudEventsMetrics metrics, CloudEventsConfiguration config) : IEnvelopeHandler
{
    static readonly ILog Log = LogManager.GetLogger<CloudEventHttpBinaryEnvelopeHandler>();

    public (Dictionary<string, string> headers, ReadOnlyMemory<byte> body) UnwrapEnvelope(
        string nativeMessageId, IDictionary<string, string> incomingHeaders,
        ContextBag extensions, ReadOnlyMemory<byte> incomingBody)
    {
        var caseInsensitiveHeaders = ToCaseInsensitiveDictionary(incomingHeaders);
        ThrowIfInvalidMessage(caseInsensitiveHeaders);
        var headers = ExtractHeaders(caseInsensitiveHeaders);
        return (headers, incomingBody);
    }

    static Dictionary<string, string> ToCaseInsensitiveDictionary(IDictionary<string, string> incomingHeaders)
    {
        return incomingHeaders
            .ToDictionary(p => p.Key.ToLowerInvariant(), p => p.Value,
                StringComparer.OrdinalIgnoreCase);
    }

    Dictionary<string, string> ExtractHeaders(IDictionary<string, string> existingHeaders)
    {
        var headersCopy = existingHeaders.ToDictionary(k => k.Key, k => k.Value);

        headersCopy[Headers.MessageId] = ExtractId(existingHeaders);
        headersCopy[Headers.ReplyToAddress] = ExtractSource(existingHeaders);
        headersCopy[Headers.EnclosedMessageTypes] = ExtractType(existingHeaders);
        if (existingHeaders.TryGetValue(CloudEventHttpBinaryEnvelopeHandlerConstants.TimeProperty, out var time))
        {
            headersCopy[Headers.TimeSent] = time;
        }

        return headersCopy;
    }

    string ExtractType(IDictionary<string, string> existingHeaders)
    {
        var cloudEventType = ExtractHeader(existingHeaders, CloudEventHttpBinaryEnvelopeHandlerConstants.TypeProperty);
        return config.TypeMappings.TryGetValue(cloudEventType, out var typeMapping)
            ? string.Join(',', typeMapping)
            : cloudEventType;
    }

    static string ExtractId(IDictionary<string, string> existingHeaders) =>
        ExtractHeader(existingHeaders, CloudEventHttpBinaryEnvelopeHandlerConstants.IdProperty);

    static string ExtractSource(IDictionary<string, string> existingHeaders) =>
        ExtractHeader(existingHeaders, CloudEventHttpBinaryEnvelopeHandlerConstants.SourceProperty);

    static string ExtractHeader(IDictionary<string, string> existingHeaders, string property) =>
        existingHeaders[property];

    void ThrowIfInvalidMessage(IDictionary<string, string> headers)
    {
        if (!HasRequiredHeaders(headers))
        {
            metrics.RecordValidMessage(false, CloudEventsMetrics.CloudEventTypes.HTTP_BINARY);
            throw new NotSupportedException(
                $"Missing headers: {string.Join(",", CloudEventHttpBinaryEnvelopeHandlerConstants.RequiredHeaders.Where(h => !headers.ContainsKey(h)))}");
        }

        metrics.RecordValidMessage(true, CloudEventsMetrics.CloudEventTypes.HTTP_BINARY);

        if (headers.TryGetValue(CloudEventHttpBinaryEnvelopeHandlerConstants.VersionProperty, out var version))
        {
            if (version != CloudEventHttpBinaryEnvelopeHandlerConstants.SupportedVersion)
            {
                metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.HTTP_BINARY, version);

                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("Unexpected CloudEvent version property value {0} for message {1}",
                        version, headers[CloudEventHttpBinaryEnvelopeHandlerConstants.IdProperty]);
                }
            }
            else
            {
                metrics.RecordUnexpectedVersion(true, CloudEventsMetrics.CloudEventTypes.HTTP_BINARY, CloudEventHttpBinaryEnvelopeHandlerConstants.SupportedVersion);
            }
        }
        else
        {
            metrics.RecordUnexpectedVersion(false, CloudEventsMetrics.CloudEventTypes.HTTP_BINARY, null);

            if (Log.IsWarnEnabled)
            {
                Log.WarnFormat("CloudEvent version property is missing for message id {0}", headers[CloudEventHttpBinaryEnvelopeHandlerConstants.IdProperty]);
            }
        }
    }

    static bool HasRequiredHeaders(IDictionary<string, string> incomingHeaders) => CloudEventHttpBinaryEnvelopeHandlerConstants.RequiredHeaders.All(incomingHeaders.ContainsKey);

    public bool CanUnwrapEnvelope(string nativeMessageId, IDictionary<string, string> incomingHeaders,
        ContextBag extensions, ReadOnlyMemory<byte> incomingBody) =>
        CloudEventHttpBinaryEnvelopeHandlerConstants.RequiredHeaders.All(incomingHeaders.ContainsKey);
}