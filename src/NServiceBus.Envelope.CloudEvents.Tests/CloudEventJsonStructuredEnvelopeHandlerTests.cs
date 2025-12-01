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
class CloudEventJsonStructuredEnvelopeHandlerTests
{
    internal required TestMeterFactory meterFactory;
    internal required string testEndpointName;
    internal required string NativeMessageId;
    internal required Dictionary<string, string> NativeHeaders;
    internal required Dictionary<string, object> Payload;
    internal required ReadOnlyMemory<byte> Body;
    internal required CloudEventJsonStructuredEnvelopeHandler envelopeHandler;
    internal required MetricCollector<long> invalidMessageCounter;
    internal required MetricCollector<long> unexpectedVersionCounter;

    [SetUp]
    public void SetUp()
    {
        NativeMessageId = Guid.NewGuid().ToString();
        Payload = new Dictionary<string, object>
        {
            ["type"] = "com.example.someevent",
            ["source"] = "/mycontext",
            ["id"] = Guid.NewGuid().ToString(),
            ["some_other_property"] = "some_other_value",
            ["data"] = "{}",
            ["datacontenttype"] = "application/json",
            ["time"] = "2023-10-10T10:00:00Z",
            ["specversion"] = "1.0"
        };

        NativeHeaders = new Dictionary<string, string>
        {
            [Headers.ContentType] = "application/cloudevents+json",
        };
        Body = new ReadOnlyMemory<byte>();
        testEndpointName = "testEndpointName";
        meterFactory = new();

        invalidMessageCounter = new MetricCollector<long>(meterFactory, "NServiceBus.Envelope.CloudEvents",
            "nservicebus.envelope.cloud_events.received.invalid_message");

        unexpectedVersionCounter = new MetricCollector<long>(meterFactory, "NServiceBus.Envelope.CloudEvents",
            "nservicebus.envelope.cloud_events.received.unexpected_version");

        envelopeHandler = new CloudEventJsonStructuredEnvelopeHandler(new(meterFactory, testEndpointName));
    }

    [Test]
    public void Should_unmarshal_regular_json()
    {
        var cloudEventBody = new Dictionary<string, string>
        {
            ["property"] = "value"
        };
        Payload["datacontenttype"] = "application/json";
        Payload["data"] = cloudEventBody;

        IncomingMessage actual = RunEnvelopHandlerTest();

        Assert.Multiple(() =>
        {
            AssertTypicalFields(actual);
            Assert.That(actual.Body.Span.SequenceEqual(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(cloudEventBody))));
        });
    }

    [Test]
    public void Should_unmarshal_regular_xml()
    {
        Payload["datacontenttype"] = "application/xml";
        Payload["data"] = "<much wow=\"xml\"/>";

        IncomingMessage actual = RunEnvelopHandlerTest();

        Assert.Multiple(() =>
        {
            AssertTypicalFields(actual);
            Assert.That(actual.Body.Span.SequenceEqual(Encoding.UTF8.GetBytes(Payload["data"].ToString()!)));
        });
    }

    [Test]
    public void Should_unmarshal_base64_binary()
    {
        var rawPayload = "<much wow=\"xml\"/>";
        Payload["datacontenttype"] = "application/xml";
        Payload["data_base64"] = Convert.ToBase64String(Encoding.UTF8.GetBytes(rawPayload));

        IncomingMessage actual = RunEnvelopHandlerTest();

        Assert.Multiple(() =>
        {
            AssertTypicalFields(actual);
            Assert.That(actual.Body.Span.SequenceEqual(Encoding.UTF8.GetBytes(rawPayload)));
        });
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_regular_message()
    {
        IncomingMessage actual = RunEnvelopHandlerTest();

        var invalidMessageCounterSnapshot = invalidMessageCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(invalidMessageCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(invalidMessageCounterSnapshot[0].Value, Is.EqualTo(0));
            Assert.That(invalidMessageCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(testEndpointName));
            Assert.That(invalidMessageCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED));
        });
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_message_with_version()
    {
        IncomingMessage actual = RunEnvelopHandlerTest();

        var unexpectedVersionCounterSnapshot = unexpectedVersionCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(unexpectedVersionCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Value, Is.EqualTo(0));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(testEndpointName));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.version"],
                Is.EqualTo("1.0"));
        });
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_message_without_version()
    {
        Payload.Remove("specversion");
        IncomingMessage actual = RunEnvelopHandlerTest();

        var unexpectedVersionCounterSnapshot = unexpectedVersionCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(unexpectedVersionCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Value, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(testEndpointName));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.version"],
                Is.EqualTo(null));
        });
    }

    [Test]
    public void Should_report_metric_when_unmarshaling_message_with_unrecognized_version()
    {
        Payload["specversion"] = "wrong";
        IncomingMessage actual = RunEnvelopHandlerTest();

        var unexpectedVersionCounterSnapshot = unexpectedVersionCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(unexpectedVersionCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Value, Is.EqualTo(1));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(testEndpointName));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED));
            Assert.That(unexpectedVersionCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.version"],
                Is.EqualTo("wrong"));
        });
    }

    [Test]
    public void Should_support_message_with_correct_content_type()
    {
        var actual = envelopeHandler.CanUnwrapEnvelope(NativeMessageId, NativeHeaders, new ContextBag(), Body);
        Assert.That(actual, Is.True);
    }

    [Test]
    public void Should_not_support_message_with_incorrect_content_type()
    {
        NativeHeaders[Headers.ContentType] = "wrong_content";
        var actual = envelopeHandler.CanUnwrapEnvelope(NativeMessageId, NativeHeaders, new ContextBag(), Body);
        Assert.That(actual, Is.False);
    }

    [Test]
    [TestCase("type")]
    [TestCase("id")]
    [TestCase("source")]
    [TestCase("datacontenttype")]
    public void Should_throw_when_property_is_missing(string property) =>
        Assert.Throws<NotSupportedException>(() =>
        {
            Payload.Remove(property);
            RunEnvelopHandlerTest();
        });

    [Test]
    public void Should_throw_when_data_properties_are_missing() =>
        Assert.Throws<NotSupportedException>(() =>
        {
            Payload.Remove("data");
            Payload.Remove("data_base64");
            RunEnvelopHandlerTest();
        });

    [Test]
    [TestCase("type")]
    [TestCase("id")]
    [TestCase("source")]
    [TestCase("datacontenttype")]
    [TestCase("data")]
    public void Should_record_metric_when_property_is_missing(string property)
    {
        try
        {
            Payload.Remove(property);
            RunEnvelopHandlerTest();
        }
        catch { }

        var invalidMessageCounterSnapshot = invalidMessageCounter.GetMeasurementSnapshot();

        Assert.Multiple(() =>
        {
            Assert.That(invalidMessageCounterSnapshot.Count, Is.EqualTo(1));
            Assert.That(invalidMessageCounterSnapshot[0].Value, Is.EqualTo(1));
            Assert.That(invalidMessageCounterSnapshot[0].Tags["nservicebus.endpoint"], Is.EqualTo(testEndpointName));
            Assert.That(invalidMessageCounterSnapshot[0].Tags["nservicebus.envelope.cloud_events.received.envelope_type"],
                Is.EqualTo(CloudEventsMetrics.CloudEventTypes.JSON_STRUCTURED));
        });
    }

    [Test]
    public void Should_not_throw_when_data_property_is_present() =>
        Assert.DoesNotThrow(() =>
        {
            Payload["data"] = "{}";
            Payload.Remove("data_base64");
            RunEnvelopHandlerTest();
        });

    [Test]
    public void Should_not_throw_when_data_base64_property_is_present() =>
        Assert.DoesNotThrow(() =>
        {
            Payload["data_base64"] = "e30=";
            Payload.Remove("data");
            RunEnvelopHandlerTest();
        });

    IncomingMessage RunEnvelopHandlerTest()
    {
        string serializedBody = JsonSerializer.Serialize(Payload);
        var fullBody = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(serializedBody));
        (Dictionary<string, string> convertedHeader, ReadOnlyMemory<byte> convertedBody) = envelopeHandler.UnwrapEnvelope(NativeMessageId, NativeHeaders, new ContextBag(), fullBody);
        return new IncomingMessage(NativeMessageId, convertedHeader, convertedBody);
    }

    void AssertTypicalFields(IncomingMessage actual)
    {
        Assert.Multiple(() =>
        {
            Assert.That(actual.MessageId, Is.EqualTo(Payload["id"]));
            Assert.That(actual.NativeMessageId, Is.EqualTo(NativeMessageId));
            Assert.That(actual.Headers[Headers.MessageId], Is.EqualTo(Payload["id"]));
            Assert.That(actual.Headers[Headers.ReplyToAddress], Is.EqualTo(Payload["source"]));
            Assert.That(actual.Headers[Headers.TimeSent], Is.EqualTo(Payload["time"]));
            Assert.That(actual.Headers["id"], Is.EqualTo(Payload["id"]));
            Assert.That(actual.Headers["type"], Is.EqualTo(Payload["type"]));
            Assert.That(actual.Headers["source"], Is.EqualTo(Payload["source"]));
            Assert.That(actual.Headers["datacontenttype"], Is.EqualTo(Payload["datacontenttype"]));
            Assert.That(actual.Headers["some_other_property"], Is.EqualTo(Payload["some_other_property"]));
            Assert.That(actual.Headers.ContainsKey("data"), Is.False);
            Assert.That(actual.Headers.ContainsKey("data_base64"), Is.False);
        });
    }
}