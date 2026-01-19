namespace NServiceBus.Envelope.CloudEvents.Tests;

using System;
using System.Buffers;
using System.Collections.Generic;
using Microsoft.Extensions.Diagnostics.Metrics.Testing;
using System.Text;
using System.Text.Json;
using Extensibility;
using Fakes;
using NServiceBus;
using NUnit.Framework;

[TestFixture]
class CloudEventAmqpBinaryEnvelopeHandlerTests
{
    internal required TestMeterFactory MeterFactory;
    internal required string TestEndpointName;
    internal required string NativeMessageId;
    internal required Dictionary<string, string?> NativeHeaders;
    internal required CloudEventsConfiguration cloudEventsConfiguration;
    internal required CloudEventAmqpBinaryEnvelopeHandler EnvelopeHandler;
    internal required MetricCollector<long> InvalidMessageCounter;
    internal required MetricCollector<long> UnexpectedVersionCounter;
    internal required MetricCollector<long> AttemptCounter;
    internal required ReadOnlyMemory<byte> Body;

    class MyEvent
    {
    }

    [SetUp]
    public void SetUp()
    {
        NativeMessageId = Guid.NewGuid().ToString();
        cloudEventsConfiguration = new CloudEventsConfiguration
        {
            TypeMappings =
            {
                { "com.example.someevent", [typeof(MyEvent)]}
            }
        };
        NativeHeaders = new Dictionary<string, string?>
        {
            ["cloudEvents:type"] = "com.example.someevent",
            ["cloudEvents:source"] = "/mycontext",
            ["cloudEvents:id"] = NativeMessageId,
            ["cloudEvents:time"] = "2023-10-10T10:00:00Z",
            ["cloudEvents:specversion"] = "1.0"
        };
        Body = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["some_other_property"] = "some_other_value",
            ["data"] = "{}",
        })));
        TestEndpointName = "testEndpointName";
        MeterFactory = new();

        AttemptCounter = new MetricCollector<long>(MeterFactory, "NServiceBus.Envelope.CloudEvents",
            "nservicebus.envelope.cloud_events.received.unwrapping_attempt");

        InvalidMessageCounter = new MetricCollector<long>(MeterFactory, "NServiceBus.Envelope.CloudEvents",
            "nservicebus.envelope.cloud_events.received.invalid_message");

        UnexpectedVersionCounter = new MetricCollector<long>(MeterFactory, "NServiceBus.Envelope.CloudEvents",
            "nservicebus.envelope.cloud_events.received.unexpected_version");

        EnvelopeHandler = new CloudEventAmqpBinaryEnvelopeHandler(new(MeterFactory, TestEndpointName), cloudEventsConfiguration);
    }

    [Test]
    public void Should_unmarshal_regular_message()
    {
        (Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body) actual = RunEnvelopHandlerTest()!.Value;

        Assert.Multiple(() =>
        {
            AssertTypicalFields(actual);
            Assert.That(actual.Body.Span.SequenceEqual(Body.Span));
        });
    }

    [Test]
    public void Should_ignore_missing_time()
    {
        NativeHeaders.Remove("time");

        (Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body) actual = RunEnvelopHandlerTest()!.Value;

        AssertTypicalFields(actual, shouldHaveTime: false);
    }

    [Test]
    public void Should_ignore_empty_time()
    {
        NativeHeaders["time"] = "";

        (Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body) actual = RunEnvelopHandlerTest()!.Value;

        AssertTypicalFields(actual, shouldHaveTime: false);
    }

    [Test]
    public void Should_ignore_null_time()
    {
        NativeHeaders["time"] = null;

        (Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body) actual = RunEnvelopHandlerTest()!.Value;

        AssertTypicalFields(actual, shouldHaveTime: false);
    }

    [Test]
    public void Should_ignore_string_null_time()
    {
        NativeHeaders["time"] = "null";

        (Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body) actual = RunEnvelopHandlerTest()!.Value;

        AssertTypicalFields(actual, shouldHaveTime: false);
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_regular_message()
    {
        (Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body) actual = RunEnvelopHandlerTest()!.Value;

        var invalidMessageCounterSnapshot = InvalidMessageCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(invalidMessageCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(invalidMessageCounterSnapshot[0].Value, Is.EqualTo(0));
            Assert.That(invalidMessageCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(TestEndpointName));
            Assert.That(invalidMessageCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.AMQP_BINARY));
        });
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_message_with_version()
    {
        (Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body) actual = RunEnvelopHandlerTest()!.Value;

        var unexpectedVersionCounterSnapshot = UnexpectedVersionCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(unexpectedVersionCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Value, Is.EqualTo(0));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(TestEndpointName));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.AMQP_BINARY));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.version"],
                Is.EqualTo("1.0"));
        });
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_message_without_version()
    {
        NativeHeaders.Remove("cloudEvents:specversion");
        (Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body) actual = RunEnvelopHandlerTest()!.Value;

        var unexpectedVersionCounterSnapshot = UnexpectedVersionCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(unexpectedVersionCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Value, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(TestEndpointName));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.AMQP_BINARY));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.version"],
                Is.EqualTo(null));
        });
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_message_with_unrecognized_version()
    {
        NativeHeaders["cloudEvents:specversion"] = "wrong";
        (Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body) actual = RunEnvelopHandlerTest()!.Value;

        var unexpectedVersionCounterSnapshot = UnexpectedVersionCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(unexpectedVersionCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Value, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(TestEndpointName));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.AMQP_BINARY));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.version"],
                Is.EqualTo("wrong"));
        });
    }

    [Test]
    [TestCase("cloudEvents:type")]
    [TestCase("cloudEvents:id")]
    [TestCase("cloudEvents:source")]
    public void Should_return_null_when_property_is_missing(string property)
    {
        NativeHeaders.Remove(property);

        (Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body)? actual = RunEnvelopHandlerTest();

        Assert.That(actual, Is.Null);
    }

    [Test]
    [TestCase("cloudEvents:type")]
    [TestCase("cloudEvents:id")]
    [TestCase("cloudEvents:source")]
    public void Should_not_record_metric_when_property_is_missing(string property)
    {
        NativeHeaders.Remove(property);
        RunEnvelopHandlerTest();

        var invalidMessageCounterSnapshot = InvalidMessageCounter.GetMeasurementSnapshot();

        Assert.That(invalidMessageCounterSnapshot.Count, Is.EqualTo(0));
    }

    (Dictionary<string, string> headers, ReadOnlyMemory<byte> body)? RunEnvelopHandlerTest()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        var upperCaseHeaders = NativeHeaders.ToDictionary(k => k.Key.ToUpper(), k => k.Value);
        var headers = EnvelopeHandler.UnwrapEnvelope(NativeMessageId, upperCaseHeaders!, Body.Span, new ContextBag(), bodyWriter);
        return headers == null ? null : (headers, bodyWriter.WrittenMemory);
    }

    void AssertTypicalFields((Dictionary<string, string> Headers, ReadOnlyMemory<byte> Body) actual, bool shouldHaveTime = true)
    {
        var attemptCounterSnapshot = AttemptCounter.GetMeasurementSnapshot();
        Assert.Multiple(() =>
        {
            Assert.That(actual.Headers[Headers.MessageId], Is.EqualTo(NativeMessageId));
            Assert.That(actual.Headers[Headers.ReplyToAddress], Is.EqualTo(NativeHeaders["cloudEvents:source"]));
            if (shouldHaveTime)
            {
                Assert.That(actual.Headers[Headers.TimeSent], Is.EqualTo("2023-10-10 10:00:00:000000 Z"));
            }

            Assert.That(actual.Headers.ContainsKey("data"), Is.False);
            Assert.That(actual.Headers.ContainsKey("some_other_property"), Is.False);
            Assert.That(actual.Headers[Headers.EnclosedMessageTypes], Is.EqualTo("NServiceBus.Envelope.CloudEvents.Tests.CloudEventAmqpBinaryEnvelopeHandlerTests+MyEvent"));

            Assert.That(attemptCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(attemptCounterSnapshot[0].Value, Is.EqualTo(1));
            Assert.That(attemptCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(TestEndpointName));
            Assert.That(attemptCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.AMQP_BINARY));
        });
    }
}