namespace NServiceBus.Envelope.CloudEvents.Tests;

using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using Extensibility;
using Fakes;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using NServiceBus;
using NUnit.Framework;
using Transport;

[TestFixture]
class CloudEventHttpBinaryEnvelopeHandlerTests
{
    internal required TestMeterFactory MeterFactory;
    internal required string TestEndpointName;
    internal required string NativeMessageId;
    internal required Dictionary<string, string> NativeHeaders;
    internal required ReadOnlyMemory<byte> Body;
    internal required CloudEventHttpBinaryEnvelopeHandler EnvelopeHandler;
    internal required MetricCollector<long> InvalidMessageCounter;
    internal required MetricCollector<long> UnexpectedVersionCounter;

    [SetUp]
    public void SetUp()
    {
        NativeMessageId = Guid.NewGuid().ToString();
        NativeHeaders = new Dictionary<string, string>
        {
            ["ce-type"] = "com.example.someevent",
            ["ce-source"] = "/mycontext",
            ["ce-id"] = NativeMessageId,
            ["ce-time"] = "2023-10-10T10:00:00Z",
            ["ce-specversion"] = "1.0"
        };
        Body = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["some_other_property"] = "some_other_value",
            ["data"] = "{}",
        })));
        TestEndpointName = "testEndpointName";
        MeterFactory = new();

        InvalidMessageCounter = new MetricCollector<long>(MeterFactory, "NServiceBus.Envelope.CloudEvents",
            "nservicebus.envelope.cloud_events.received.invalid_message");

        UnexpectedVersionCounter = new MetricCollector<long>(MeterFactory, "NServiceBus.Envelope.CloudEvents",
            "nservicebus.envelope.cloud_events.received.unexpected_version");

        EnvelopeHandler = new CloudEventHttpBinaryEnvelopeHandler(new(MeterFactory, TestEndpointName));
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
    public void Should_report_metric_when_unmarshaling_regular_message()
    {
        IncomingMessage actual = RunEnvelopHandlerTest();

        var invalidMessageCounterSnapshot = InvalidMessageCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(invalidMessageCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(invalidMessageCounterSnapshot[0].Value, Is.EqualTo(0));
            Assert.That(invalidMessageCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(TestEndpointName));
            Assert.That(invalidMessageCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.HTTP_BINARY));
        });
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_message_with_version()
    {
        IncomingMessage actual = RunEnvelopHandlerTest();

        var unexpectedVersionCounterSnapshot = UnexpectedVersionCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(unexpectedVersionCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Value, Is.EqualTo(0));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(TestEndpointName));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.HTTP_BINARY));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.version"],
                Is.EqualTo("1.0"));
        });
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_message_without_version()
    {
        NativeHeaders.Remove("ce-specversion");
        IncomingMessage actual = RunEnvelopHandlerTest();

        var unexpectedVersionCounterSnapshot = UnexpectedVersionCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(unexpectedVersionCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Value, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(TestEndpointName));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.HTTP_BINARY));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.version"],
                Is.EqualTo(null));
        });
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_message_with_unrecognized_version()
    {
        NativeHeaders["ce-specversion"] = "wrong";
        IncomingMessage actual = RunEnvelopHandlerTest();

        var unexpectedVersionCounterSnapshot = UnexpectedVersionCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(unexpectedVersionCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Value, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(TestEndpointName));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.HTTP_BINARY));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.version"],
                Is.EqualTo("wrong"));
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
    [TestCase("ce-type")]
    [TestCase("ce-id")]
    [TestCase("ce-source")]
    public void Should_record_metric_when_property_is_missing(string property)
    {
        try
        {
            NativeHeaders.Remove(property);
            RunEnvelopHandlerTest();
        }
        catch { }

        var invalidMessageCounterSnapshot = InvalidMessageCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(invalidMessageCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(invalidMessageCounterSnapshot[0].Value, Is.EqualTo(1));
            Assert.That(invalidMessageCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(TestEndpointName));
            Assert.That(invalidMessageCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.HTTP_BINARY));
        });
    }

    [Test]
    public void Should_support_message_with_correct_headers()
    {
        var actual = EnvelopeHandler.CanUnwrapEnvelope(NativeMessageId, NativeHeaders, new ContextBag(), Body);
        Assert.That(actual, Is.True);
    }

    [Test]
    [TestCase("ce-type")]
    [TestCase("ce-id")]
    [TestCase("ce-source")]
    public void Should_not_support_message_with_missing_headers(string property)
    {
        NativeHeaders.Remove(property);
        var actual = EnvelopeHandler.CanUnwrapEnvelope(NativeMessageId, NativeHeaders, new ContextBag(), Body);
        Assert.That(actual, Is.False);
    }

    IncomingMessage RunEnvelopHandlerTest()
    {
        var upperCaseHeaders = NativeHeaders.ToDictionary(k => k.Key.ToUpper(), k => k.Value);
        (Dictionary<string, string> convertedHeader, ReadOnlyMemory<byte> convertedBody) = EnvelopeHandler.UnwrapEnvelope(NativeMessageId, upperCaseHeaders, new ContextBag(), Body);
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