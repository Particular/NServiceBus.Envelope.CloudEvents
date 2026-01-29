namespace NServiceBus.Envelope.CloudEvents;

/// <summary>
/// Defines the behavior of the JSON structured envelope unwrapper.
/// </summary>
public enum JsonStructureEnvelopeHandlingMode
{
    /// <summary>
    /// The JSON structured envelope unwrapper will try to unwrap the envelope only if
    /// the Content-Type header is present and matches the expected value. 
    /// </summary>
    Strict = 0,

    /// <summary>
    /// The JSON structured envelope unwrapper will try to unwrap the envelope even if
    /// the Content-Type header is not present and will try to determine the
    /// envelope type based on the JSON structure.
    /// </summary>
    Permissive = 1
}