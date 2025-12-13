namespace NServiceBus.Envelope.CloudEvents;

using System.Collections;

/// <summary>
/// Defines the list of the current envelope unwrappers.
/// </summary>
public class EnvelopeUnwrappers : IEnumerable<EnvelopeUnwrapper>
{
    internal OrderedDictionary<Type, EnvelopeUnwrapper> Unwrappers { get; } = new()
    {
        { typeof(CloudEventJsonStructuredEnvelopeUnwrapper), new CloudEventJsonStructuredEnvelopeUnwrapper() },
        { typeof(CloudEventHttpBinaryEnvelopeUnwrapper), new CloudEventHttpBinaryEnvelopeUnwrapper() },
        { typeof(CloudEventAmqpBinaryEnvelopeUnwrapper), new CloudEventAmqpBinaryEnvelopeUnwrapper() }
    };

    /// <summary>
    /// Finds an envelope unwrapper given its type.
    /// </summary>
    /// <typeparam name="TUnwrapper">The unwrapper type.</typeparam>
    /// <returns>The envelope unwrapper or <c>null</c> if no unwrapper con be found.</returns>
    public TUnwrapper FindEnvelopeUnwrapper<TUnwrapper>() where TUnwrapper : EnvelopeUnwrapper => (TUnwrapper)Unwrappers[typeof(TUnwrapper)];

    /// <summary>
    /// Clear the envelope unwrappers. 
    /// </summary>
    public void Clear() => Unwrappers.Clear();

    /// <summary>
    /// Add a new envelope unwrapper.
    /// </summary>
    public void Add(EnvelopeUnwrapper unwrapper) => Unwrappers.Add(unwrapper.GetType(), unwrapper);

    /// <summary>
    /// Remove the given envelope unwrapper.
    /// </summary>
    public void Remove(EnvelopeUnwrapper unwrapper) => Unwrappers.Remove(unwrapper.GetType());

    /// <summary>
    /// Insert a new envelope unwrapper at the given position
    /// </summary>
    /// <param name="index"></param>
    /// <param name="unwrapper"></param>
    public void InsertAt(int index, EnvelopeUnwrapper unwrapper) => Unwrappers.Insert(index, unwrapper.GetType(), unwrapper);

    /// <summary>
    /// Returns an EnvelopeUnwrapper IEnumerator.
    /// </summary>
    public IEnumerator<EnvelopeUnwrapper> GetEnumerator() => ((IEnumerable<EnvelopeUnwrapper>)Unwrappers.Values).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}