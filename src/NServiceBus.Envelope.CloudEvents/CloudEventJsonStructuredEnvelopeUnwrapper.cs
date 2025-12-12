namespace NServiceBus.Envelope.CloudEvents;

using Features;

/// <summary>
/// Unwrapper for JSON Structured cloud events envelopes.
/// </summary>
public class CloudEventJsonStructuredEnvelopeUnwrapper() : EnvelopeUnwrapper
{
    internal override void RegisterUnwrapper(FeatureConfigurationContext context) => RegisterUnwrapper<CloudEventJsonStructuredEnvelopeHandler>(context);

    /// <summary>
    /// Determines the envelope handling behavior. In strict mode the unwrapper expects the correct Content-Type header. In permissive mode
    /// </summary>
    public JsonStructureEnvelopeHandlingMode EnvelopeHandlingMode { get; set; } = JsonStructureEnvelopeHandlingMode.Strict;
}