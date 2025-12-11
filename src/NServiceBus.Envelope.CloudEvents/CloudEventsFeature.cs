namespace NServiceBus.Envelope.CloudEvents;

using System.Diagnostics.Metrics;
using Features;
using Microsoft.Extensions.DependencyInjection;
using NServiceBus;

class CloudEventsFeature : Feature
{
    protected override void Setup(FeatureConfigurationContext context)
    {
        var cloudEventsConfiguration = context.Settings.Get<CloudEventsConfiguration>(CloudEventsEndpointConfigurationExtensions.CloudEventsSetting);
        _ = context.Services.AddSingleton(cloudEventsConfiguration);
        _ = context.Services.AddSingleton(sp =>
        {
            var endpointName = context.Settings.EndpointName();
            return new CloudEventsMetrics(sp.GetRequiredService<IMeterFactory>(), endpointName);
        });

        foreach (var unwrapper in cloudEventsConfiguration.EnvelopeUnwrappers)
        {
            _ = context.Services.AddSingleton(typeof(IEnvelopeHandler), unwrapper.UnwrapperType);
        }

        context.Settings.AddStartupDiagnosticsSection("NServiceBus.Envelope.CloudEvents",
            new
            {
                RegisteredEnvelopeHandlers = new[]
                {
                    nameof(CloudEventJsonStructuredEnvelopeHandler),
                    nameof(CloudEventAmqpBinaryEnvelopeHandler),
                    nameof(CloudEventHttpBinaryEnvelopeHandler)
                },
                // TODO list here all the settings from CloudEventsConfiguration
                Configuration = ""
            });
    }
}