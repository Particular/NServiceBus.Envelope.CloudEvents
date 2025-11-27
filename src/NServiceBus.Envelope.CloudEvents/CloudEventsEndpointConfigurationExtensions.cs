namespace NServiceBus;

using Configuration.AdvancedExtensibility;

/// <summary>
/// 
/// </summary>
public static class CloudEventsEndpointConfigurationExtensions
{
    internal const string CloudEventsSetting = "NServiceBus.Envelope.CloudEvents";
    /// <summary>
    /// Add CloudEvents envelopes support to this endpoint
    /// </summary>
    /// <param name="configuration">The current endpoint configuration</param>
    /// <returns>The <see cref="CloudEventsConfiguration"/> instance to customize CloudEvents support</returns>
    public static CloudEventsConfiguration EnableCloudEvents(this EndpointConfiguration configuration)
    {
        var config = new CloudEventsConfiguration();
        var settings = configuration.GetSettings();
        settings.Set(CloudEventsSetting, config);
        return config;
    }
}