using NServiceBus.AcceptanceTesting.Customization;
using NServiceBus.AcceptanceTesting.Support;
using NServiceBus.AcceptanceTests.Routing;
using NServiceBus.AcceptanceTests.Routing.NativePublishSubscribe;
using NServiceBus.AcceptanceTests.Sagas;
using NServiceBus.AcceptanceTests.Versioning;
using NServiceBus.Envelope.CloudEvents.ASB.AcceptanceTests;
using NServiceBus.Transport.AzureServiceBus;
using System.IO.Hashing;
using System.Text;
using NUnit.Framework;
using Conventions = NServiceBus.AcceptanceTesting.Customization.Conventions;

public class ConfigureEndpointAzureServiceBusTransport : IConfigureEndpointTestExecution
{
    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings, PublisherMetadata publisherMetadata)
    {
        var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString");

        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("envvar AzureServiceBus_ConnectionString not set");
        }

        var topology = TopicTopology.Default;
        topology.OverrideSubscriptionNameFor(endpointName, endpointName.Shorten());

        foreach (var eventType in publisherMetadata.Publishers.SelectMany(p => p.Events))
        {
            topology.PublishTo(eventType, eventType.ToTopicName());
            topology.SubscribeTo(eventType, eventType.ToTopicName());
        }

        ApplyMappingsToSupportMultipleInheritance(endpointName, topology);

        var transport = new AzureServiceBusTransport(connectionString, topology)
        {
            // Shared scenarios hard-code some endpoint names, so every parallel fixture gets its own hierarchy below the namespace.
            HierarchyNamespaceOptions = new HierarchyNamespaceOptions { HierarchyNamespace = FixtureToken() }
        };

        configuration.UseTransport(transport);
        configuration.EnableCloudEvents();

        var recoverability = configuration.Recoverability();
        recoverability.Immediate(config => config.NumberOfRetries(0));
        recoverability.Delayed(config => config.NumberOfRetries(0));

        configuration.EnableTestIndependence();
        configuration.EnforcePublisherMetadataRegistration(endpointName, publisherMetadata);

        return Task.CompletedTask;
    }

    static void ApplyMappingsToSupportMultipleInheritance(string endpointName, TopicPerEventTopology topology)
    {
        if (endpointName == Conventions.EndpointNamingConvention(typeof(MultiSubscribeToPolymorphicEvent.Subscriber)))
        {
            topology.SubscribeTo<MultiSubscribeToPolymorphicEvent.IMyEvent>(typeof(MultiSubscribeToPolymorphicEvent.MyEvent1).ToTopicName());
            topology.SubscribeTo<MultiSubscribeToPolymorphicEvent.IMyEvent>(typeof(MultiSubscribeToPolymorphicEvent.MyEvent2).ToTopicName());
        }

        if (endpointName == Conventions.EndpointNamingConvention(typeof(When_subscribing_to_a_base_event.GeneralSubscriber)))
        {
            topology.SubscribeTo<When_subscribing_to_a_base_event.IBaseEvent>(typeof(When_subscribing_to_a_base_event.SpecificEvent).ToTopicName());
        }

        if (endpointName == Conventions.EndpointNamingConvention(typeof(When_publishing_an_event_implementing_two_unrelated_interfaces.Subscriber)))
        {
            topology.SubscribeTo<When_publishing_an_event_implementing_two_unrelated_interfaces.IEventA>(
                typeof(When_publishing_an_event_implementing_two_unrelated_interfaces.CompositeEvent).ToTopicName());
            topology.SubscribeTo<When_publishing_an_event_implementing_two_unrelated_interfaces.IEventB>(
                typeof(When_publishing_an_event_implementing_two_unrelated_interfaces.CompositeEvent).ToTopicName());
        }

        if (endpointName == Conventions.EndpointNamingConvention(typeof(When_started_by_base_event_from_other_saga.SagaThatIsStartedByABaseEvent)))
        {
            topology.SubscribeTo<When_started_by_base_event_from_other_saga.IBaseEvent>(
                typeof(When_started_by_base_event_from_other_saga.ISomethingHappenedEvent).ToTopicName());
        }

        if (endpointName == Conventions.EndpointNamingConvention(typeof(When_multiple_versions_of_a_message_is_published.V1Subscriber)))
        {
            topology.SubscribeTo<When_multiple_versions_of_a_message_is_published.V1Event>(
                typeof(When_multiple_versions_of_a_message_is_published.V2Event).ToTopicName());
        }
    }

    public Task Cleanup() => Task.CompletedTask;

    // Lowercase because the dead-letter tests compare entity paths against lowercased endpoint names.
    static string FixtureToken()
    {
        var className = TestContext.CurrentContext.Test.ClassName;
        if (string.IsNullOrEmpty(className))
        {
            throw new InvalidOperationException("Entity names depend on the running fixture and can only be resolved while a test is running.");
        }

        return XxHash32.HashToUInt32(Encoding.UTF8.GetBytes(className)).ToString("x8");
    }
}