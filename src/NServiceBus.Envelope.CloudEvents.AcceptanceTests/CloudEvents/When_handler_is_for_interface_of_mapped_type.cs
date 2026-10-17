namespace NServiceBus.AcceptanceTests.CloudEvents
{
    using AcceptanceTesting;
    using Configuration.AdvancedExtensibility;
    using EndpointTemplates;
    using NServiceBus.Pipeline;
    using NUnit.Framework;
    using Transport;
    using Unicast.Messages;
    using UnscannedConcreteMessages;

    // ConcreteEvent is declared outside of the test class on purpose: the acceptance testing framework
    // only scans types nested in the test class, so the concrete type is not known to the
    // MessageMetadataRegistry at startup. The handler is registered for the IOrderEvent interface,
    // which makes only the interface known to the registry.
    public class When_handler_is_for_interface_of_mapped_type : NServiceBusAcceptanceTest
    {
        const string CloudEventType = "com.example.order";

        [Test]
        public async Task Without_cloud_events_the_message_is_handled()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<Endpoint>(b => b
                    .DoNotFailOnErrorMessages()
                    .When(s => s.SendLocal(new ConcreteEvent { Value = "hello" })))
                .Run();

            Assert.That(context.Handled, Is.True, context.Exception?.ToString());
        }

        [Test]
        public async Task With_cloud_events_the_message_should_be_handled()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<CloudEventsEndpoint>(b => b
                    .DoNotFailOnErrorMessages()
                    .When(SendCloudEvent))
                .Run();

            Assert.That(context.Handled, Is.True, context.Exception?.ToString());
        }

        [Test]
        public async Task With_cloud_events_and_dynamic_type_loading_disabled_the_message_is_handled()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<CloudEventsEndpointWithoutDynamicTypeLoading>(b => b
                    .DoNotFailOnErrorMessages()
                    .When(SendCloudEvent))
                .Run();

            // The assembly qualified name cannot be loaded dynamically, the mapped type must be registered
            // by the CloudEvents feature for the message to be handled.
            Assert.That(context.Handled, Is.True, context.Exception?.ToString());
        }

        [Test]
        public async Task With_cloud_events_and_registered_type_the_message_is_handled()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<CloudEventsEndpointWithRegisteredType>(b => b
                    .DoNotFailOnErrorMessages()
                    .When(SendCloudEvent))
                .Run();

            // Explicitly registering the mapped type keeps working.
            Assert.That(context.Handled, Is.True, context.Exception?.ToString());
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
                Data = new ConcreteEvent { Value = "hello" }
            }, options);
        }

        static void ConfigureCommon(EndpointConfiguration c, Context testContext) =>
            c.Recoverability().Failed(f => f.OnMessageSentToErrorQueue((failure, _) =>
            {
                testContext.Exception = failure.Exception;
                testContext.MarkAsCompleted();
                return Task.CompletedTask;
            }));

        public class Context : ScenarioContext
        {
            public bool Handled { get; set; }
            public Exception Exception { get; set; }
        }

        public class Endpoint : EndpointConfigurationBuilder
        {
            public Endpoint() =>
                EndpointSetup<DefaultServer>((c, r) => ConfigureCommon(c, (Context)r.ScenarioContext));

            [Handler]
            public class OrderHandler(Context testContext) : IHandleMessages<IOrderEvent>
            {
                public Task Handle(IOrderEvent message, IMessageHandlerContext context)
                {
                    testContext.Handled = true;
                    testContext.MarkAsCompleted();
                    return Task.CompletedTask;
                }
            }
        }

        public class CloudEventsEndpoint : EndpointConfigurationBuilder
        {
            public CloudEventsEndpoint() =>
                EndpointSetup<DefaultServer>((c, r) =>
                {
                    ConfigureCommon(c, (Context)r.ScenarioContext);
                    c.GetCloudEventsConfiguration().TypeMappings.Add(CloudEventType, [typeof(ConcreteEvent)]);
                    c.Pipeline.Register(new CloudEventsHeadersBehavior(), "Replaces NServiceBus headers with the CloudEvents content type");
                });

            [Handler]
            public class OrderHandler(Context testContext) : IHandleMessages<IOrderEvent>
            {
                public Task Handle(IOrderEvent message, IMessageHandlerContext context)
                {
                    testContext.Handled = true;
                    testContext.MarkAsCompleted();
                    return Task.CompletedTask;
                }
            }
        }

        public class CloudEventsEndpointWithRegisteredType : EndpointConfigurationBuilder
        {
            public CloudEventsEndpointWithRegisteredType() =>
                EndpointSetup<DefaultServer>((c, r) =>
                {
                    ConfigureCommon(c, (Context)r.ScenarioContext);
                    c.GetCloudEventsConfiguration().TypeMappings.Add(CloudEventType, [typeof(ConcreteEvent)]);
                    c.Pipeline.Register(new CloudEventsHeadersBehavior(), "Replaces NServiceBus headers with the CloudEvents content type");
                    c.GetSettings().GetOrCreate<MessageMetadataRegistry>().RegisterMessageTypes([typeof(ConcreteEvent)]);
                });

            [Handler]
            public class OrderHandler(Context testContext) : IHandleMessages<IOrderEvent>
            {
                public Task Handle(IOrderEvent message, IMessageHandlerContext context)
                {
                    testContext.Handled = true;
                    testContext.MarkAsCompleted();
                    return Task.CompletedTask;
                }
            }
        }

        public class CloudEventsEndpointWithoutDynamicTypeLoading : EndpointConfigurationBuilder
        {
            public CloudEventsEndpointWithoutDynamicTypeLoading() =>
                EndpointSetup<DefaultServer>((c, r) =>
                {
                    ConfigureCommon(c, (Context)r.ScenarioContext);
                    c.UseSerialization<SystemJsonSerializer>().DisableDynamicTypeLoading();
                    c.GetCloudEventsConfiguration().TypeMappings.Add(CloudEventType, [typeof(ConcreteEvent)]);
                    c.Pipeline.Register(new CloudEventsHeadersBehavior(), "Replaces NServiceBus headers with the CloudEvents content type");
                });

            [Handler]
            public class OrderHandler(Context testContext) : IHandleMessages<IOrderEvent>
            {
                public Task Handle(IOrderEvent message, IMessageHandlerContext context)
                {
                    testContext.Handled = true;
                    testContext.MarkAsCompleted();
                    return Task.CompletedTask;
                }
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
            public ConcreteEvent Data { get; set; }
        }
    }
}

namespace NServiceBus.AcceptanceTests.CloudEvents.UnscannedConcreteMessages
{
    public interface IOrderEvent : IMessage;

    public class ConcreteEvent : IOrderEvent
    {
        public string Value { get; set; }
    }
}
