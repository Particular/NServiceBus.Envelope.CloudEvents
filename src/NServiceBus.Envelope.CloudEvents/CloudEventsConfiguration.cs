namespace NServiceBus;

using Envelope.CloudEvents;

/// <summary>
/// TODO
/// </summary>
public class CloudEventsConfiguration
{
    /// <summary>
    /// Initialize a new instance of the cloud events configuration class.
    /// </summary>
    public CloudEventsConfiguration() => ResetEnvelopUnwrappers(
    [
            new CloudEventJsonStructuredEnvelopeUnwrapper(),
            new CloudEventHttpBinaryEnvelopeUnwrapper(),
            new CloudEventAmqpBinaryEnvelopeUnwrapper()
        ]);

    /// <summary>
    /// Specify type mappings. Allows the user to map string values from the `type` property in incoming cloud events to a type string that NServiceBus expects.
    /// </summary>
    public Dictionary<string, Type[]> TypeMappings { get; } = [];

    /// <summary>
    /// Resets the unwrappers according to the provided list.
    /// </summary>
    /// <param name="envelopeUnwrappers"></param>
    public void ResetEnvelopUnwrappers(List<EnvelopeUnwrapper> envelopeUnwrappers)
    {
        EnvelopeUnwrappers.Clear();
        foreach (var unwrapper in envelopeUnwrappers)
        {
            EnvelopeUnwrappers.Add(unwrapper.GetType(), unwrapper);
        }
    }

    /// <summary>
    /// Finds an envelope unwrapper given its type.
    /// </summary>
    /// <typeparam name="TUnwrapper">The unwrapper type.</typeparam>
    /// <returns>The envelope unwrapper or <c>null</c> if no unwrapper con be found.</returns>
    public TUnwrapper FindEnvelopeUnwrapper<TUnwrapper>() where TUnwrapper : EnvelopeUnwrapper => (TUnwrapper)EnvelopeUnwrappers[typeof(TUnwrapper)];

    internal OrderedDictionary<Type, EnvelopeUnwrapper> EnvelopeUnwrappers { get; } = [];
}