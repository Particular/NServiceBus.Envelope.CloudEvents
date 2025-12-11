namespace NServiceBus.Envelope.CloudEvents;

public class CloudEventAmqpBinaryEnvelopeUnwrapper() : EnvelopeUnwrapper(typeof(CloudEventAmqpBinaryEnvelopeHandler));