namespace NServiceBus.Envelope.CloudEvents.Tests;

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using NServiceBus;
using Extensibility;
using NUnit.Framework;
using Transport;

[TestFixture]
public class CloudEventHttpBinaryEnvelopeHandlerTests
{
    readonly string NativeMessageId = Guid.NewGuid().ToString();
    Dictionary<string, string> NativeHeaders;
    ReadOnlyMemory<byte> Body;
    CloudEventHttpBinaryEnvelopeHandler envelopeHandler;

    [SetUp]
    public void SetUp()
    {
        NativeHeaders = new Dictionary<string, string>
        {
            ["ce-type"] = "com.example.someevent",
            ["ce-source"] = "/mycontext",
            ["ce-id"] = NativeMessageId,
            ["ce-time"] = "2023-10-10T10:00:00Z"
        };
        Body = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["some_other_property"] = "some_other_value",
            ["data"] = "{}",
        })));
        envelopeHandler = new CloudEventHttpBinaryEnvelopeHandler();
    }

    [Test]
    public void Should_unmarshal_regular_message()
    {
        IncomingMessage actual = RunEnvelopHandlerTest();

        Assert.Multiple(() =>
        {
            AssertTypicalFields(actual);
            Assert.That(actual.Body.Span.SequenceEqual(Body.Span));
        });
    }

    [Test]
    [TestCase("ce-type")]
    [TestCase("ce-id")]
    [TestCase("ce-source")]
    public void Should_throw_when_property_is_missing(string property)
    {
        Assert.Throws<NotSupportedException>(() =>
        {
            NativeHeaders.Remove(property);
            RunEnvelopHandlerTest();
        });
    }

    [Test]
    public void Should_support_message_with_correct_headers()
    {
        var actual = envelopeHandler.CanUnwrapEnvelope(NativeMessageId, NativeHeaders, new ContextBag(), Body);
        Assert.That(actual, Is.True);
    }

    [Test]
    [TestCase("cloudEvents:type")]
    [TestCase("cloudEvents:id")]
    [TestCase("cloudEvents:source")]
    public void Should_not_support_message_with_missing_headers(string property)
    {
        NativeHeaders.Remove(property);
        var actual = envelopeHandler.CanUnwrapEnvelope(NativeMessageId, NativeHeaders, new ContextBag(), Body);
        Assert.That(actual, Is.False);
    }

    IncomingMessage RunEnvelopHandlerTest()
    {
        (Dictionary<string, string> convertedHeader, ReadOnlyMemory<byte> convertedBody) = envelopeHandler.UnwrapEnvelope(NativeMessageId, NativeHeaders, null, Body);
        return new IncomingMessage(NativeMessageId, convertedHeader, convertedBody);
    }

    void AssertTypicalFields(IncomingMessage actual)
    {
        Assert.Multiple(() =>
        {
            Assert.That(actual.MessageId, Is.EqualTo(NativeMessageId));
            Assert.That(actual.NativeMessageId, Is.EqualTo(NativeMessageId));
            Assert.That(actual.Headers[Headers.MessageId], Is.EqualTo(NativeMessageId));
            Assert.That(actual.Headers[Headers.ReplyToAddress], Is.EqualTo(NativeHeaders["ce-source"]));
            Assert.That(actual.Headers[Headers.TimeSent], Is.EqualTo(NativeHeaders["ce-time"]));
            Assert.That(actual.Headers.ContainsKey("data"), Is.False);
            Assert.That(actual.Headers.ContainsKey("some_other_property"), Is.False);
        });
    }
}