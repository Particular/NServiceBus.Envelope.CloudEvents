namespace NServiceBus.AcceptanceTests;

using AcceptanceTesting.Support;

public class ConfigureEndpointAcceptanceTestingTransportWithCloudEvents(
    bool useNativePubSub,
    bool useNativeDelayedDelivery,
    TransportTransactionMode? transactionMode = null,
    bool? enforcePublisherMetadata = null)
    : IConfigureEndpointTestExecution
{
    readonly ConfigureEndpointAcceptanceTestingTransport transport = new(useNativePubSub, useNativeDelayedDelivery, transactionMode, enforcePublisherMetadata);

    public Task Cleanup() => transport.Cleanup();

    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings,
        PublisherMetadata publisherMetadata)
    {
        configuration.EnableCloudEvents();
        return transport.Configure(endpointName, configuration, settings, publisherMetadata);
    }
}