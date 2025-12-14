namespace NServiceBus.Envelope.CloudEvents;

using Features;

/// <summary>
/// Unwrapper for HTTP Binary cloud events envelopes.
/// </summary>
public class CloudEventHttpBinaryEnvelopeUnwrapper() : EnvelopeUnwrapper
{
    internal override void RegisterUnwrapper(FeatureConfigurationContext context, Action<object> unwrapperDiagnosticWriter)
    {
        RegisterUnwrapper<CloudEventHttpBinaryEnvelopeHandler>(context);
        unwrapperDiagnosticWriter(new { EnvelopeHandler = typeof(CloudEventHttpBinaryEnvelopeHandler) });
    }
}