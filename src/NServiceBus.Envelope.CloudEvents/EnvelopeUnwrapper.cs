namespace NServiceBus;

using Features;
using Microsoft.Extensions.DependencyInjection;

public abstract class EnvelopeUnwrapper(Type unwrapperType)
{
    internal void RegisterUnwrapper(FeatureConfigurationContext context)
    {
        _ = context.Services.AddSingleton(typeof(IEnvelopeHandler), unwrapperType);
    }
}