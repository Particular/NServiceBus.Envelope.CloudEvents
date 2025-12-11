namespace NServiceBus.Envelope.CloudEvents;

public class CloudEventHttpBinaryEnvelopeUnwrapper() : EnvelopeUnwrapper(typeof(CloudEventHttpBinaryEnvelopeHandler));