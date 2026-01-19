namespace NServiceBus.Envelope.CloudEvents;

using Features;

/// <summary>
/// Unwrapper for AMQP Binary cloud events envelopes.
/// </summary>
public class CloudEventAmqpBinaryEnvelopeUnwrapper() : EnvelopeUnwrapper
{
    internal override void RegisterUnwrapper(FeatureConfigurationContext context, Action<object> unwrapperDiagnosticWriter)
    {
        RegisterUnwrapper<CloudEventAmqpBinaryEnvelopeHandler>(context);
        unwrapperDiagnosticWriter(new { EnvelopeHandler = typeof(CloudEventAmqpBinaryEnvelopeHandler) });
    }
}