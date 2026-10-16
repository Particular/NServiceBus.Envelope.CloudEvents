namespace NServiceBus.Envelope.CloudEvents;

using System.Collections.Concurrent;

/// <summary>
/// Configuration settings for CloudEvents envelope handling.
/// </summary>
public class CloudEventsConfiguration
{
    /// <summary>
    /// Specify type mappings. Allows the user to map string values from the `type` property in incoming cloud events to a type string that NServiceBus expects.
    /// </summary>
    public Dictionary<string, Type[]> TypeMappings { get; } = new([], StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The envelope unwrappers to use to handle incoming message envelops.
    /// </summary>
    public EnvelopeUnwrappers EnvelopeUnwrappers { get; } = [];

    readonly ConcurrentDictionary<string, string> enclosedMessageTypesCache = new(StringComparer.OrdinalIgnoreCase);

    // Assembly qualified names allow Core to load the mapped types even when they are not registered
    // in the message metadata registry, and ';' is the separator Core expects for multiple types.
    internal string GetEnclosedMessageTypes(string cloudEventType) =>
        TypeMappings.TryGetValue(cloudEventType, out var typeMapping)
            ? enclosedMessageTypesCache.GetOrAdd(cloudEventType, static (_, types) => string.Join(';', types.Select(t => t.AssemblyQualifiedName)), typeMapping)
            : cloudEventType;
}