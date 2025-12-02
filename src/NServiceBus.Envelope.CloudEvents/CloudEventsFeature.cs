namespace NServiceBus.Envelope.CloudEvents;

using System.Diagnostics.Metrics;
using Features;
using Microsoft.Extensions.DependencyInjection;
using NServiceBus;

class CloudEventsFeature : Feature
{
    protected override void Setup(FeatureConfigurationContext context)
    {
        context.Services.AddSingleton<CloudEventsMetrics>(sp =>
        {
            var endpointName = context.Settings.EndpointName();
            return new CloudEventsMetrics(sp.GetRequiredService<IMeterFactory>(), endpointName);
        });
        context.Services.AddSingleton<IEnvelopeHandler, CloudEventJsonStructuredEnvelopeHandler>();
        context.Services.AddSingleton<IEnvelopeHandler, CloudEventAmqpBinaryEnvelopeHandler>();
        context.Services.AddSingleton<IEnvelopeHandler, CloudEventHttpBinaryEnvelopeHandler>();

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