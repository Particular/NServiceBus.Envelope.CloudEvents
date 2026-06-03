// ReSharper disable once CheckNamespace
namespace NServiceBus;

using Configuration.AdvancedExtensibility;
using Envelope.CloudEvents;

/// <summary>
/// Provide methods for configuring cloud events
/// </summary>
public static class CloudEventsEndpointConfigurationExtensions
{
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
            settings.Set(config);
            return config;
        }
    }
}