namespace NServiceBus.AcceptanceTests;

using AcceptanceTesting.Support;

public partial class TestSuiteConstraints
{
    public bool SupportsCrossQueueTransactions => false;

    public bool SupportsDtc => false;

    public bool SupportsDelayedDelivery => true;

    public bool SupportsNativePubSub => true;

    public bool SupportsOutbox => false;

    public bool SupportsPurgeOnStartup => false;

    public IConfigureEndpointTestExecution CreateTransportConfiguration() => new ConfigureEndpointAcceptanceTestingTransportWithCloudEvents(true, true);

    public IConfigureEndpointTestExecution CreatePersistenceConfiguration() => new ConfigureEndpointAcceptanceTestingPersistence();
}