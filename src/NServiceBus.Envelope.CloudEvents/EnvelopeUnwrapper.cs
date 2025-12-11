namespace NServiceBus;

public abstract class EnvelopeUnwrapper(Type unwrapperType)
{
    internal Type UnwrapperType{ get; init; } = unwrapperType;
}