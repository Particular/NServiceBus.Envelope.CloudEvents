namespace NServiceBus.Envelope.CloudEvents;

public class CloudEventJsonStructuredEnvelopeUnwrapper() : EnvelopeUnwrapper(typeof(CloudEventJsonStructuredEnvelopeUnwrapper))
{
    public EnvelopeHandlingMode EnvelopeHandlingMode { get; set; } = EnvelopeHandlingMode.Strict;
}

public class CloudEventHttpBinaryEnvelopeUnwrapper() : EnvelopeUnwrapper(typeof(CloudEventHttpBinaryEnvelopeHandler));
public class CloudEventAmqpBinaryEnvelopeUnwrapper() : EnvelopeUnwrapper(typeof(CloudEventAmqpBinaryEnvelopeHandler));