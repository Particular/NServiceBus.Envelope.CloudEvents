// ReSharper disable once CheckNamespace
namespace NServiceBus;

using Configuration.AdvancedExtensibility;
using Envelope.CloudEvents;

/// <summary>
/// 
/// </summary>
public static class CloudEventsEndpointConfigurationExtensions
{
    internal const string CloudEventsConfigurationSettingsKey = "NServiceBus.Envelope.CloudEvents.Configuration";

    extension(EndpointConfiguration configuration)
    {
        /// <summary>
        /// Add CloudEvents envelopes support to this endpoint
        /// </summary>
        /// <returns>The <see cref="CloudEventsConfiguration"/> instance to customize CloudEvents support</returns>
        public CloudEventsConfiguration EnableCloudEvents()
        {
            configuration.EnableFeature<CloudEventsFeature>();

            var config = new CloudEventsConfiguration();
            var settings = configuration.GetSettings();
            settings.Set(CloudEventsConfigurationSettingsKey, config);
            return config;
        }
    }
}