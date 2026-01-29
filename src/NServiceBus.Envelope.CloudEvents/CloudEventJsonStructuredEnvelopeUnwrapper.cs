namespace NServiceBus.Envelope.CloudEvents;

using Features;

/// <summary>
/// Unwrapper for JSON Structured cloud events envelopes.
/// </summary>
public class CloudEventJsonStructuredEnvelopeUnwrapper : EnvelopeUnwrapper
{
    internal override void RegisterUnwrapper(FeatureConfigurationContext context, Action<object> unwrapperDiagnosticWriter)
    {
        RegisterUnwrapper<CloudEventJsonStructuredEnvelopeHandler>(context);
        unwrapperDiagnosticWriter(new
        {
            EnvelopeHandler = typeof(CloudEventJsonStructuredEnvelopeHandler),
            EnvelopeHandlingMode = EnvelopeHandlingMode.ToString()
        });
    }

    /// <summary>
    /// Determines the envelope handling behavior. In strict mode the unwrapper expects the correct
    /// Content-Type header. In permissive mode it always tries to parse the incoming envelope.
    /// </summary>
    public JsonStructureEnvelopeHandlingMode EnvelopeHandlingMode { get; set; } = JsonStructureEnvelopeHandlingMode.Strict;
}