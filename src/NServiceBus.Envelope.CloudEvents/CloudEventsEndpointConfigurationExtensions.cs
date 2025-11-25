namespace NServiceBus;

using Configuration.AdvancedExtensibility;

/// <summary>
/// TODO
/// </summary>
public static class CloudEventsEndpointConfigurationExtensions
{
    internal const string CloudEventsSetting = "NServiceBus.Envelope.CloudEvents";
    /// <summary>
    /// TODO
    /// </summary>
    /// <param name="configuration">TODO</param>
    /// <returns>TODO</returns>
    public static CloudEventsConfiguration EnableCloudEvents(this EndpointConfiguration configuration)
    {
        var config = new CloudEventsConfiguration();
        var settings = configuration.GetSettings();
        settings.Set(CloudEventsSetting, config);
        return config;
    }
}