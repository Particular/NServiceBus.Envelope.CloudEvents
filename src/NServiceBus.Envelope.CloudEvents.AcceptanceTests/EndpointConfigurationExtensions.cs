namespace NServiceBus.AcceptanceTests;

using Configuration.AdvancedExtensibility;
using Envelope.CloudEvents;

static class EndpointConfigurationExtensions
{
    extension(EndpointConfiguration endpointConfiguration)
    {
        public CloudEventsConfiguration GetCloudEventsConfiguration() => endpointConfiguration.GetSettings().Get<CloudEventsConfiguration>(CloudEventsEndpointConfigurationExtensions.CloudEventsConfigurationSettingsKey);
    }
}