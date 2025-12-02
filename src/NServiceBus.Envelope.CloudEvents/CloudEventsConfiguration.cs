namespace NServiceBus;

/// <summary>
/// TODO
/// </summary>
public class CloudEventsConfiguration
{
    /// <summary>
    /// Specify type mappings. Allows the user to map string values from the `type` property in incoming cloud events to a type string that NServiceBus expects.
    /// </summary>
    public Dictionary<string, string>? TypeMappings { get; set; }
}