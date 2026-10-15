namespace NServiceBus.AcceptanceTests.CloudEvents
{
    using AcceptanceTesting;
    using Configuration.AdvancedExtensibility;
    using EndpointTemplates;
    using NServiceBus.Pipeline;
    using NUnit.Framework;
    using Transport;
    using Unicast.Messages;
    using UnscannedMessages;

    // UnscannedMessage is declared outside of the test class on purpose: the acceptance testing
    // framework only scans types nested in the test class, so the message type is not known to the
    // MessageMetadataRegistry at startup and there is no handler for it. This mimics an endpoint
    // receiving a known message type for which no handler has been registered.
    public class When_mapped_type_is_not_registered : NServiceBusAcceptanceTest
    {
        const string CloudEventType = "com.example.unscanned";

        [Test]
        public async Task Without_cloud_events_it_fails_at_handler_lookup()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<Endpoint>(b => b
                    .DoNotFailOnErrorMessages()
                    .When(s => s.SendLocal(new UnscannedMessage { Value = "hello" })))
                .Run();

            using (Assert.EnterMultipleScope())
            {
                // Regular NServiceBus messages carry the assembly qualified name, Type.GetType resolves it,
                // deserialization succeeds and the pipeline fails later because there is no handler.
                Assert.That(context.EnclosedMessageTypes, Is.EqualTo(typeof(UnscannedMessage).AssemblyQualifiedName));
                Assert.That(context.Exception, Is.Not.InstanceOf<MessageDeserializationException>());
                Assert.That(context.Exception.Message, Does.Contain("No handlers could be found"));
            }
        }

        [Test]
        public async Task With_cloud_events_it_should_fail_at_handler_lookup()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<CloudEventsEndpoint>(b => b
                    .DoNotFailOnErrorMessages()
                    .When(SendCloudEvent))
                .Run();

            using (Assert.EnterMultipleScope())
            {
                // The mapped type is known, so the behavior should match the non CloudEvents case.
                // Currently the envelope handler writes Type.ToString(), i.e. the full name without the
                // assembly. Type.GetType cannot load it and the registry has no metadata for it, so the
                // deserializer is invoked without any message type and fails with a MessageDeserializationException.
                Assert.That(context.Exception, Is.Not.InstanceOf<MessageDeserializationException>(), context.Exception?.ToString());
                Assert.That(context.Exception.Message, Does.Contain("No handlers could be found"));
            }
        }

        [Test]
        public async Task With_cloud_events_and_registered_type_it_fails_at_handler_lookup()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<CloudEventsEndpointWithRegisteredType>(b => b
                    .DoNotFailOnErrorMessages()
                    .When(SendCloudEvent))
                .Run();

            using (Assert.EnterMultipleScope())
            {
                // Once the type is known to the registry, the full name fallback resolves it and the
                // behavior matches the non CloudEvents case.
                Assert.That(context.EnclosedMessageTypes, Is.EqualTo(typeof(UnscannedMessage).FullName));
                Assert.That(context.Exception, Is.Not.InstanceOf<MessageDeserializationException>());
                Assert.That(context.Exception.Message, Does.Contain("No handlers could be found"));
            }
        }

        static Task SendCloudEvent(IMessageSession session)
        {
            // The cloud event id becomes the NServiceBus message id once unwrapped, it must match the
            // outgoing message id header, see CloudEventsHeadersBehavior.
            var id = Guid.NewGuid().ToString();
            var options = new SendOptions();
            options.RouteToThisEndpoint();
            options.SetMessageId(id);

            return session.Send(new CloudEventMessage
            {
                SpecVersion = "1.0",
                Type = CloudEventType,
                Source = "/acceptance-tests",
                Id = id,
                Time = "2026-10-15T10:00:00Z",
                Data = new UnscannedMessage { Value = "hello" }
            }, options);
        }

        static void ConfigureCommon(EndpointConfiguration c, Context testContext)
        {
            c.Pipeline.Register(new CaptureEnclosedMessageTypesBehavior(testContext), "Captures the unwrapped enclosed message types header");
            c.Recoverability().Failed(f => f.OnMessageSentToErrorQueue((failure, _) =>
            {
                testContext.Exception = failure.Exception;
                testContext.MarkAsCompleted();
                return Task.CompletedTask;
            }));
        }

        static void ConfigureCloudEvents(EndpointConfiguration c)
        {
            c.GetCloudEventsConfiguration().TypeMappings.Add(CloudEventType, [typeof(UnscannedMessage)]);
            c.Pipeline.Register(new CloudEventsHeadersBehavior(), "Replaces NServiceBus headers with the CloudEvents content type");
        }

        public class Context : ScenarioContext
        {
            public string EnclosedMessageTypes { get; set; }
            public Exception Exception { get; set; }
        }

        public class Endpoint : EndpointConfigurationBuilder
        {
            public Endpoint() =>
                EndpointSetup<DefaultServer>((c, r) => ConfigureCommon(c, (Context)r.ScenarioContext));
        }

        public class CloudEventsEndpoint : EndpointConfigurationBuilder
        {
            public CloudEventsEndpoint() =>
                EndpointSetup<DefaultServer>((c, r) =>
                {
                    ConfigureCommon(c, (Context)r.ScenarioContext);
                    ConfigureCloudEvents(c);
                });
        }

        public class CloudEventsEndpointWithRegisteredType : EndpointConfigurationBuilder
        {
            public CloudEventsEndpointWithRegisteredType() =>
                EndpointSetup<DefaultServer>((c, r) =>
                {
                    ConfigureCommon(c, (Context)r.ScenarioContext);
                    ConfigureCloudEvents(c);
                    c.GetSettings().GetOrCreate<MessageMetadataRegistry>().RegisterMessageTypes([typeof(UnscannedMessage)]);
                });
        }

        class CaptureEnclosedMessageTypesBehavior(Context testContext) : IBehavior<IIncomingPhysicalMessageContext, IIncomingPhysicalMessageContext>
        {
            public Task Invoke(IIncomingPhysicalMessageContext context, Func<IIncomingPhysicalMessageContext, Task> next)
            {
                testContext.EnclosedMessageTypes = context.Message.Headers.GetValueOrDefault(Headers.EnclosedMessageTypes);
                return next(context);
            }
        }

        class CloudEventsHeadersBehavior : IBehavior<IDispatchContext, IDispatchContext>
        {
            // Ensures the outgoing message looks like a native CloudEvent, without NServiceBus headers.
            // The message id header is preserved because recoverability reads it from the wrapped message
            // while the pipeline reads it from the unwrapped one; keeping them equal allows the acceptance
            // testing framework to correlate the failure with the recoverability action.
            public Task Invoke(IDispatchContext context, Func<IDispatchContext, Task> next)
            {
                OutgoingMessage outgoingMessage = context.Operations.First().Message;

                Dictionary<string, string> headers = outgoingMessage.Headers;
                var messageId = headers[Headers.MessageId];
                headers.Clear();
                headers[Headers.MessageId] = messageId;
                headers[Headers.ContentType] = "application/cloudevents+json; charset=utf-8";

                return next(context);
            }
        }

        public class CloudEventMessage : IMessage
        {
            public string SpecVersion { get; set; }
            public string Type { get; set; }
            public string Source { get; set; }
            public string Id { get; set; }
            public string Time { get; set; }
            public UnscannedMessage Data { get; set; }
        }
    }
}

namespace NServiceBus.AcceptanceTests.CloudEvents.UnscannedMessages
{
    public class UnscannedMessage : IMessage
    {
        public string Value { get; set; }
    }
}
