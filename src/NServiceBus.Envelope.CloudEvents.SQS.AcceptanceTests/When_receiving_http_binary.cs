namespace NServiceBus.AcceptanceTests;

using System.Text.Json;
using AcceptanceTesting;
using AcceptanceTesting.Customization;
using Amazon.SQS.Model;
using Configuration.AdvancedExtensibility;
using EndpointTemplates;
using Envelope.CloudEvents;
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

    [Test, CancelAfter(120_000)]
    public async Task Should_receive_message(CancellationToken cancellationToken = default)
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<Receiver>(c => c.When(async _ =>
            {
                await SendTo<Receiver>(headers, body).ConfigureAwait(false);
            }))
            .Done(c => c.Received)
            .Run(cancellationToken);

        Assert.That(context.Received, Is.True);
        Assert.That(context.ReceivedMessage, Is.Not.Null);
        Assert.That(context.ReceivedMessage.AppInfoA, Is.EqualTo("abc"));
        Assert.That(context.ReceivedMessage.AppInfoB, Is.EqualTo(123));
        Assert.That(context.ReceivedMessage.AppInfoC, Is.EqualTo(true));
    }

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
        public Receiver() => EndpointSetup<DefaultServer>(c =>
        {
            var jsonSerializerOptions = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };
            c.UseSerialization<SystemJsonSerializer>().Options(jsonSerializerOptions);

            var settings = c.GetSettings();
            var config = settings.Get<CloudEventsConfiguration>(CloudEventsEndpointConfigurationExtensions.CloudEventsSetting);
            config.TypeMappings.Add("com.example.someevent", [typeof(Message)]);
        });

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