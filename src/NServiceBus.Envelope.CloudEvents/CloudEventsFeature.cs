namespace NServiceBus.Envelope.CloudEvents;

using System.Diagnostics.Metrics;
using Features;
using Microsoft.Extensions.DependencyInjection;
using Unicast.Messages;

class CloudEventsFeature : Feature
{
    protected override void Setup(FeatureConfigurationContext context)
    {
        var cloudEventsConfiguration = context.Settings.Get<CloudEventsConfiguration>().Clone();

        // Mapped types might not be discovered by assembly scanning or registered through handlers,
        // e.g. when handlers are registered for one of their interfaces. Registering them ensures
        // the types can be resolved when dynamic type loading is disabled.
        context.Settings.Get<MessageMetadataRegistry>().RegisterMessageTypes(cloudEventsConfiguration.TypeMappings.Values.SelectMany(types => types));
        _ = context.Services.AddSingleton(cloudEventsConfiguration);
        _ = context.Services.AddSingleton(sp =>
        {
            var endpointName = context.Settings.EndpointName();
            return new CloudEventsMetrics(sp.GetRequiredService<IMeterFactory>(), endpointName);
        });

        List<object> unwrappersDiagnostics = [];
        foreach (var unwrapper in cloudEventsConfiguration.EnvelopeUnwrappers)
        {
            unwrapper.RegisterUnwrapper(context, obj => unwrappersDiagnostics.Add(obj));
        }

        context.Settings.AddStartupDiagnosticsSection("NServiceBus.Envelope.CloudEvents",
            new
            {
                RegisteredEnvelopeHandlers = unwrappersDiagnostics,
                cloudEventsConfiguration.TypeMappings
            });
    }
}