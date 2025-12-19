namespace NServiceBus.Envelope.CloudEvents;

using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;

sealed class CloudEventsMetrics : IDisposable
{
    internal static class CloudEventTypes
    {
        internal static readonly string JSON_STRUCTURED_STRICT = "JSON_structured_strict";
        internal static readonly string JSON_STRUCTURED_PERMISSIVE = "JSON_structured_permissive";
        internal static readonly string HTTP_BINARY = "HTTP_binary";
        internal static readonly string AMQP_BINARY = "AMQP_binary";
    }

    public CloudEventsMetrics(IMeterFactory meterFactory, string endpointName)
    {
        this.endpointName = endpointName;

        meter = meterFactory.Create("NServiceBus.Envelope.CloudEvents", "0.2.0");

        invalidMessageCounter = meter.CreateCounter<long>(
            "nservicebus.envelope.cloud_events.received.invalid_message",
            description: "Number of invalid messages received.");

        unexpectedVersionCounter = meter.CreateCounter<long>(
            "nservicebus.envelope.cloud_events.received.unexpected_version",
            description: "Number of received messages with unrecognized version type.");

        unwrappingAttemptCounter = meter.CreateCounter<long>(
            "nservicebus.envelope.cloud_events.received.unwrapping_attempt",
            description: "Total number of unwrapping attempts.");
    }

    public void RecordValidMessage(bool isValid, string envelopeType)
    {
        if (!invalidMessageCounter.Enabled)
        {
            return;
        }

        TagList tags;
        tags.Add("nservicebus.endpoint", endpointName);
        tags.Add("nservicebus.envelope.cloud_events.received.envelope_type", envelopeType);

        invalidMessageCounter.Add(isValid ? 0 : 1, tags);
    }

    public void RecordUnexpectedVersion(bool isExpectedVersion, string envelopeType, string? version)
    {
        if (!unexpectedVersionCounter.Enabled)
        {
            return;
        }

        TagList tags;
        tags.Add("nservicebus.endpoint", endpointName);
        tags.Add("nservicebus.envelope.cloud_events.received.envelope_type", envelopeType);
        tags.Add("nservicebus.envelope.cloud_events.received.version", version);

        unexpectedVersionCounter.Add(isExpectedVersion ? 0 : 1, tags);
    }

    public void RecordUnwrappingAttempt(bool attemptsToUnwrap, string envelopeType)
    {
        if (!unwrappingAttemptCounter.Enabled)
        {
            return;
        }

        TagList tags;
        tags.Add("nservicebus.endpoint", endpointName);
        tags.Add("nservicebus.envelope.cloud_events.received.envelope_type", envelopeType);

        unwrappingAttemptCounter.Add(attemptsToUnwrap ? 1 : 0, tags);
    }

    public void Dispose() => meter.Dispose();

    readonly Counter<long> invalidMessageCounter;
    readonly Counter<long> unexpectedVersionCounter;
    readonly Counter<long> unwrappingAttemptCounter;
    readonly string endpointName;
    readonly Meter meter;
}