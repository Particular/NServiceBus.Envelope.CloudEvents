namespace NServiceBus.Envelope.CloudEvents.AcceptanceTests.CloudEvents;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NServiceBus.AcceptanceTesting;
using NServiceBus.AcceptanceTests;
using NServiceBus.AcceptanceTests.EndpointTemplates;
using NServiceBus.Pipeline;
using NServiceBus.Transport;
using NUnit.Framework;

public class When_json_structured_message_received : NServiceBusAcceptanceTest
{
    [Test]
    public async Task A_structured_cloud_event_is_received()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<Endpoint>(g => g.When(b =>
            {
                // The following represents a CloudEvent that Azure Blob Storage generates
                // to notify that a new blob item has been created 
                return b.SendLocal(new Message()
                {
                    SpecVersion = "1.0",
                    Type = "Microsoft.Storage.BlobCreated",
                    Source =
                        "/subscriptions/{subscription-id}/resourceGroups/{resource-group}/providers/Microsoft.Storage/storageAccounts/{storage-account}",
                    Id = "9aeb0fdf-c01e-0131-0922-9eb54906e209",
                    Time = "2019-11-18T15:13:39.4589254Z",
                    Subject = "blobServices/default/containers/{storage-container}/blobs/{new-file}",
                    Data = new NestedData()
                    {
                        Api = "PutBlockList",
                        ClientRequestId = "4c5dd7fb-2c48-4a27-bb30-5361b5de920a",
                        RequestId = "9aeb0fdf-c01e-0131-0922-9eb549000000",
                        ETag = "0x8D76C39E4407333",
                        ContentType = "image/png",
                        ContentLength = 30699,
                        BlobType = "BlockBlob",
                        Url = "https://gridtesting.blob.core.windows.net/testcontainer/{new-file}",
                        Sequencer = "000000000000000000000000000099240000000000c41c18"
                    }
                });
            }))
            .Done(c => c.MessageReceived)
            .Run().ConfigureAwait(false);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(string.IsNullOrWhiteSpace(context.MessageId), Is.False);
            Assert.That(context.Headers[Headers.MessageId], Is.EqualTo(context.MessageId), "Should populate the NServiceBus.MessageId header with the new value");
        }
    }

    class CustomSerializationBehavior : IBehavior<IDispatchContext, IDispatchContext>
    {
        // The custom serializer is required to ensure the outgoing message contains
        // only the specific cloud events required headers and not the NServiceBus ones. 
        public Task Invoke(IDispatchContext context, Func<IDispatchContext, Task> next)
        {
            OutgoingMessage outgoingMessage = context.Operations.First().Message;

            Dictionary<string, string> headers = outgoingMessage.Headers;
            headers.Clear();
            headers[Headers.ContentType] = "application/cloudevents+json; charset=utf-8";
            // TODO remove once type decoder works
            headers[Headers.EnclosedMessageTypes] = "NServiceBus.Envelope.CloudEvents.AcceptanceTests.CloudEvents.When_json_structured_message_received+Message, NServiceBus.Envelope.CloudEvents.AcceptanceTests, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null";

            return next(context);
        }
    }

    class Context : ScenarioContext
    {
        public bool MessageReceived { get; set; }
        public string MessageId { get; set; }
        public Dictionary<string, string> Headers { get; set; }
    }

    class Endpoint : EndpointConfigurationBuilder
    {
        public Endpoint()
        {
            EndpointSetup<DefaultServer>(c =>
            {
                c.Pipeline.Register("CustomSerializationBehavior", new CustomSerializationBehavior(),
                    "Serializing message");
            });
        }

        class Handler(Context testContext) : IHandleMessages<Message>
        {
            public Task Handle(Message message, IMessageHandlerContext context)
            {
                testContext.MessageId = context.MessageId;
                testContext.Headers = context.MessageHeaders.ToDictionary(x => x.Key, x => x.Value);
                testContext.MessageReceived = true;

                return Task.CompletedTask;
            }
        }
    }

    public class NestedData
    {
        public string Api { get; set; }
        public string ClientRequestId { get; set; }
        public string RequestId { get; set; }
        public string ETag { get; set; }
        public string ContentType { get; set; }
        public int ContentLength { get; set; }
        public string BlobType { get; set; }
        public string Url { get; set; }
        public string Sequencer { get; set; }
    }

    public class Message : IMessage
    {
        public string SpecVersion { get; set; }
        public string Type { get; set; }
        public string Source { get; set; }
        public string Id { get; set; }
        public string Time { get; set; }
        public string Subject { get; set; }
        public NestedData Data { get; set; }
    }
}
