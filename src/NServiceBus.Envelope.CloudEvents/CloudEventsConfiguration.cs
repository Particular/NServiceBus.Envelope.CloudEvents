namespace NServiceBus.Envelope.CloudEvents;

/// <summary>
/// Configuration settings for CloudEvents envelope handling.
/// </summary>
public class CloudEventsConfiguration
{
    /// <summary>
    /// Specify type mappings. Allows the user to map string values from the `type` property in incoming cloud events to a type string that NServiceBus expects.
    /// </summary>
    public Dictionary<string, Type[]> TypeMappings { get; } = [];

    /// <summary>
    /// The envelope unwrappers to use to handle incoming message envelops.
    /// </summary>
    public EnvelopeUnwrappers EnvelopeUnwrappers { get; } = [];
}