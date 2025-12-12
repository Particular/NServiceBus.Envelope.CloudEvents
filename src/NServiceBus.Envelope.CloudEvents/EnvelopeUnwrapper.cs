namespace NServiceBus.Envelope.CloudEvents;

using Features;

/// <summary>
/// Base class to define envelope unwrappers.
/// </summary>
public abstract class EnvelopeUnwrapper
{
    internal abstract void RegisterUnwrapper(FeatureConfigurationContext context);
    internal void RegisterUnwrapper<THandler>(FeatureConfigurationContext context) where THandler : class, IEnvelopeHandler => context.AddEnvelopeHandler<THandler>();
}