namespace NServiceBus.Envelope.CloudEvents;

using Features;

/// <summary>
/// Unwrapper for HTTP Binary cloud events envelopes.
/// </summary>
public class CloudEventHttpBinaryEnvelopeUnwrapper() : EnvelopeUnwrapper
{
    internal override void RegisterUnwrapper(FeatureConfigurationContext context) => RegisterUnwrapper<CloudEventHttpBinaryEnvelopeHandler>(context);
}