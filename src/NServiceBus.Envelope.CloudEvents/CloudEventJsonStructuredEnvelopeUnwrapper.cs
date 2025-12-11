namespace NServiceBus.Envelope.CloudEvents;

public class CloudEventJsonStructuredEnvelopeUnwrapper() : EnvelopeUnwrapper(typeof(CloudEventJsonStructuredEnvelopeUnwrapper))
{
    // could use generics
    public EnvelopeHandlingMode EnvelopeHandlingMode { get; set; } = EnvelopeHandlingMode.Strict;
}