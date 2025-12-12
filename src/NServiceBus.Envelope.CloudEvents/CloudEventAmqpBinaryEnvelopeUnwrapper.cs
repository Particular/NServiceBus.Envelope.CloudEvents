namespace NServiceBus.Envelope.CloudEvents;

using Features;

/// <summary>
/// Unwrapper for AMQP Binary cloud events envelopes.
/// </summary>
public class CloudEventAmqpBinaryEnvelopeUnwrapper() : EnvelopeUnwrapper
{
    internal override void RegisterUnwrapper(FeatureConfigurationContext context) => RegisterUnwrapper<CloudEventAmqpBinaryEnvelopeHandler>(context);
}