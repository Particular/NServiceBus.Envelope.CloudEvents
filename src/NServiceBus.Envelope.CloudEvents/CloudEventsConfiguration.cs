namespace NServiceBus.Envelope.CloudEvents;

using System.Collections.Frozen;

/// <summary>
/// Configuration settings for CloudEvents envelope handling.
/// </summary>
public class CloudEventsConfiguration
{
    readonly FrozenDictionary<string, string> enclosedMessageTypes = FrozenDictionary<string, string>.Empty;

    /// <summary>
    /// Creates a new instance of the <see cref="CloudEventsConfiguration"/>.
    /// </summary>
    public CloudEventsConfiguration()
    {
        TypeMappings = new(StringComparer.OrdinalIgnoreCase);
        EnvelopeUnwrappers = [];
    }

    // Shields the runtime from later changes to the user-provided instance, everything the handlers need is computed here.
    CloudEventsConfiguration(CloudEventsConfiguration source)
    {
        TypeMappings = new(source.TypeMappings.Select(kvp => KeyValuePair.Create(kvp.Key, (Type[])kvp.Value.Clone())), StringComparer.OrdinalIgnoreCase);
        EnvelopeUnwrappers = source.EnvelopeUnwrappers;

        // Assembly-qualified names allow Core to load the mapped types even when they are not registered
        // in the message metadata registry, and ';' is the separator Core expects for multiple types.
        enclosedMessageTypes = TypeMappings.ToFrozenDictionary(
            kvp => kvp.Key,
            kvp => string.Join(';', kvp.Value.Select(t => t.AssemblyQualifiedName)),
            StringComparer.OrdinalIgnoreCase);
        IsStrictJsonStructuredHandling = EnvelopeUnwrappers.Find<CloudEventJsonStructuredEnvelopeUnwrapper>()?.EnvelopeHandlingMode == JsonStructureEnvelopeHandlingMode.Strict;
    }

    /// <summary>
    /// Specify type mappings. Allows the user to map string values from the `type` property in incoming cloud events to a type string that NServiceBus expects.
    /// </summary>
    public Dictionary<string, Type[]> TypeMappings { get; }

    /// <summary>
    /// The envelope unwrappers to use to handle incoming message envelops.
    /// </summary>
    public EnvelopeUnwrappers EnvelopeUnwrappers { get; }

    internal bool IsStrictJsonStructuredHandling { get; }

    internal CloudEventsConfiguration Clone() => new(this);

    internal string GetEnclosedMessageTypes(string cloudEventType) =>
        enclosedMessageTypes.GetValueOrDefault(cloudEventType, cloudEventType);
}