using NServiceBus.AcceptanceTesting.Customization;
using NServiceBus.AcceptanceTesting.Support;
using NServiceBus.Envelope.CloudEvents.ASB.AcceptanceTests;
using NServiceBus.Transport.AzureServiceBus;
using System.IO.Hashing;
using System.Text;
using NUnit.Framework;

public class ConfigureEndpointAzureServiceBusTransport : IConfigureEndpointTestExecution
{
    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings, PublisherMetadata publisherMetadata)
    {
        var connectionString = Environment.GetEnvironmentVariable("AzureServiceBus_ConnectionString");

        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("envvar AzureServiceBus_ConnectionString not set");
        }

#pragma warning disable CS0618 // Type or member is obsolete
        var topology = TopicTopology.MigrateFromSingleDefaultTopic();
#pragma warning restore CS0618 // Type or member is obsolete
        topology.OverrideSubscriptionNameFor(endpointName, endpointName.Shorten());

        foreach (var eventType in publisherMetadata.Publishers.SelectMany(p => p.Events))
        {
            topology.EventToMigrate(eventType, ruleNameOverride: eventType.FullName.Shorten());
        }

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