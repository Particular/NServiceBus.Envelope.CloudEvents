namespace NServiceBus.AcceptanceTests;

using System.Threading.Tasks;
using Amazon.SQS.Model;
using AcceptanceTesting;
using AcceptanceTesting.Customization;
using EndpointTemplates;
using Envelope.CloudEvents.SQS.AcceptanceTests;
using NUnit.Framework;

public class When_receiving_http_binary : NServiceBusAcceptanceTest
{
    const string body = """
                        {
                          "appinfoA" : "abc",
                          "appinfoB" : 123,
                          "appinfoC" : true
                        }
                        """;

    static readonly Dictionary<string, string> headers = new()
    {
        { "ce-specversion", "1.0" },
        { "ce-type", "com.example.someevent" },
        { "ce-source", "/mycontext" },
        { "ce-id", "C234-1234-1234" },
        { "ce-time", "2018-04-05T17:31:00Z" },
        { "ce-comexampleextension1", "value" },
        { "ce-comexampleothervalue", "5" },
        { "content-type", "application/json" },
    };

    [Test]
    public async Task Should_receive_message() =>
        await Scenario.Define<Context>()
            .WithEndpoint<Receiver>(c => c.When(async _ =>
            {
                await SendTo<Receiver>(headers, body).ConfigureAwait(false);
            }))
            .Done(c => c.Received)
            .Run();

    static async Task SendTo<TEndpoint>(Dictionary<string, string> messageHeaders, string messageBody)
    {
        using var sqsClient = ClientFactories.CreateSqsClient();
        var getQueueUrlResponse = await sqsClient.GetQueueUrlAsync(new GetQueueUrlRequest
        {
            QueueName = TestNameHelper.GetSqsQueueName(Conventions.EndpointNamingConvention(typeof(TEndpoint)), SetupFixture.NamePrefix)
        }).ConfigureAwait(false);

        var messageAttributes = messageHeaders.ToDictionary(
            header => header.Key,
            header => new MessageAttributeValue
            {
                StringValue = header.Value,
                DataType = "String"
            });

        // TODO: remove once we have TypeMapping
        messageAttributes.Add("NServiceBus.EnclosedMessageTypes", new MessageAttributeValue()
        {
            StringValue = typeof(Message).AssemblyQualifiedName,
            DataType = "String",
        });

        var sendMessageRequest = new SendMessageRequest
        {
            MessageAttributes = messageAttributes,
            QueueUrl = getQueueUrlResponse.QueueUrl,
            MessageBody = messageBody
        };

        _ = await sqsClient.SendMessageAsync(sendMessageRequest).ConfigureAwait(false);
    }

    public class Context : ScenarioContext
    {
        public bool Received { get; set; }
        public Message ReceivedMessage { get; set; }
    }

    public class Receiver : EndpointConfigurationBuilder
    {
        public Receiver() => EndpointSetup<DefaultServer>();

        public class MyMessageHandler : IHandleMessages<Message>
        {
            public MyMessageHandler(Context testContext) => this.testContext = testContext;

            public Task Handle(Message message, IMessageHandlerContext context)
            {
                testContext.Received = true;
                testContext.ReceivedMessage = message;
                return Task.CompletedTask;
            }

            readonly Context testContext;
        }
    }

    public class Message : ICommand
    {
        public string AppInfoA { get; set; }
        public int AppInfoB { get; set; }
        public bool AppInfoC { get; set; }
    }
}