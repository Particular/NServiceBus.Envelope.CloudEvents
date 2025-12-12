namespace NServiceBus.Envelope.CloudEvents;

using System.Diagnostics.Metrics;
using Features;
using Microsoft.Extensions.DependencyInjection;
using NServiceBus;

class CloudEventsFeature : Feature
{
    protected override void Setup(FeatureConfigurationContext context)
    {
        _ = context.Services.AddSingleton(context.Settings.Get<CloudEventsConfiguration>(CloudEventsEndpointConfigurationExtensions.CloudEventsSetting));
        _ = context.Services.AddSingleton(sp =>
        {
            var endpointName = context.Settings.EndpointName();
            return new CloudEventsMetrics(sp.GetRequiredService<IMeterFactory>(), endpointName);
        });
        context.AddEnvelopeHandler<CloudEventJsonStructuredEnvelopeHandler>();
        context.AddEnvelopeHandler<CloudEventAmqpBinaryEnvelopeHandler>();
        context.AddEnvelopeHandler<CloudEventHttpBinaryEnvelopeHandler>();

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