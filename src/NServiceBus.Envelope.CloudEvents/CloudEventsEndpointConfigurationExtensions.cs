namespace NServiceBus;

using Configuration.AdvancedExtensibility;

public static class CloudEventsEndpointConfigurationExtensions
{
    internal const string CloudEventsSetting = "NServiceBus.Envelope.CloudEvents";
    
    public static CloudEventsConfiguration EnableCloudEvents(this EndpointConfiguration configuration)
    {
        var config = new CloudEventsConfiguration();
        var settings = configuration.GetSettings();
        settings.Set(CloudEventsSetting, config);
        
        return config;
    }
}