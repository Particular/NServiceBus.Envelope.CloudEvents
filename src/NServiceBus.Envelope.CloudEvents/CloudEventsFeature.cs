namespace NServiceBus.Envelope.CloudEvents;

using Features;
using Microsoft.Extensions.DependencyInjection;
using NServiceBus;

class CloudEventsFeature : Feature
{
    protected override void Setup(FeatureConfigurationContext context)
    {
        context.Services.AddSingleton<IEnvelopeHandler, CloudEventJsonStructuredEnvelopeHandler>();
    }
}