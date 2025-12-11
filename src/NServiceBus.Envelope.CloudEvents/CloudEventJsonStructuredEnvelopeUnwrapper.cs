namespace NServiceBus.Envelope.CloudEvents;

public class CloudEventJsonStructuredEnvelopeUnwrapper() : EnvelopeUnwrapper(typeof(CloudEventJsonStructuredEnvelopeUnwrapper))
{
    public EnvelopeHandlingMode EnvelopeHandlingMode { get; set; } = EnvelopeHandlingMode.Strict;
}