namespace NServiceBus.Envelope.CloudEvents.Benchmarks;

using System.Buffers;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Extensibility;

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 10)]
public class CloudEventJsonStructuredEnvelopeHandlerBenchmarks
{
    CloudEventJsonStructuredEnvelopeHandler? _permissiveHandler;
    CloudEventJsonStructuredEnvelopeHandler? _strictHandler;
    Dictionary<string, string>? _headers;
    byte[]? _smallJsonPayload;
    byte[]? _mediumJsonPayload;
    byte[]? _largeJsonPayload;
    byte[]? _base64Payload;
    byte[]? _xmlPayload;
    string? _nativeMessageId;

    class MyEvent;

    [GlobalSetup]
    public void Setup()
    {
        _nativeMessageId = Guid.NewGuid().ToString();

        _headers = new Dictionary<string, string>
        {
            [Headers.ContentType] = "application/cloudevents+json; charset=utf8"
        };

        // Configure permissive handler
        var permissiveConfig = new CloudEventsConfiguration
        {
            TypeMappings =
            {
                { "com.example.someevent", [typeof(MyEvent)] },
            }
        };
        permissiveConfig.EnvelopeUnwrappers.Find<CloudEventJsonStructuredEnvelopeUnwrapper>()
            .EnvelopeHandlingMode = JsonStructureEnvelopeHandlingMode.Permissive;

        _permissiveHandler = new CloudEventJsonStructuredEnvelopeHandler(
            new CloudEventsMetrics(new FakeMeterFactory(), "benchmark"),
            permissiveConfig);

        // Configure strict handler
        var strictConfig = new CloudEventsConfiguration
        {
            TypeMappings =
            {
                { "com.example.someevent", [typeof(MyEvent)] },
            }
        };
        strictConfig.EnvelopeUnwrappers.Find<CloudEventJsonStructuredEnvelopeUnwrapper>()
            .EnvelopeHandlingMode = JsonStructureEnvelopeHandlingMode.Strict;

        _strictHandler = new CloudEventJsonStructuredEnvelopeHandler(
            new CloudEventsMetrics(new FakeMeterFactory(), "benchmark"),
            strictConfig);

        // Create small JSON payload (typical small message)
        _smallJsonPayload = CreateJsonPayload(new Dictionary<string, string>
        {
            ["property"] = "value"
        });

        // Create medium JSON payload (typical business message)
        _mediumJsonPayload = CreateJsonPayload(new Dictionary<string, object>
        {
            ["orderId"] = "12345",
            ["customerId"] = "67890",
            ["orderDate"] = "2024-01-13T10:00:00Z",
            ["items"] = new[]
            {
                new { productId = "P1", quantity = 2, price = 29.99 },
                new { productId = "P2", quantity = 1, price = 49.99 },
                new { productId = "P3", quantity = 3, price = 9.99 }
            },
            ["totalAmount"] = 139.94,
            ["shippingAddress"] = new
            {
                street = "123 Main St",
                city = "Springfield",
                state = "IL",
                zipCode = "62701",
                country = "USA"
            }
        });

        // Create large JSON payload (large complex message)
        var largeData = new Dictionary<string, object>
        {
            ["metadata"] = new
            {
                version = "1.0",
                timestamp = DateTime.UtcNow,
                correlationId = Guid.NewGuid().ToString()
            }
        };

        var items = new List<object>();
        for (int i = 0; i < 100; i++)
        {
            items.Add(new
            {
                id = i,
                name = $"Item {i}",
                description = $"This is a detailed description for item {i}",
                properties = new Dictionary<string, string>
                {
                    ["prop1"] = "value1",
                    ["prop2"] = "value2",
                    ["prop3"] = "value3"
                }
            });
        }
        largeData["items"] = items;
        _largeJsonPayload = CreateJsonPayload(largeData);

        // Create base64 binary payload
        var binaryData = Encoding.UTF8.GetBytes("<data>Some XML or binary content here</data>");
        _base64Payload = CreateBase64Payload(binaryData);

        // Create XML payload
        _xmlPayload = CreateXmlPayload("<order><id>12345</id><customer>John Doe</customer></order>");
    }

    byte[] CreateJsonPayload(object data)
    {
        var cloudEvent = new Dictionary<string, object>
        {
            ["TYPE"] = "com.example.someevent",
            ["SOURCE"] = "/mycontext",
            ["ID"] = Guid.NewGuid().ToString(),
            ["DATA"] = data,
            ["DATACONTENTTYPE"] = "application/json",
            ["TIME"] = "2024-01-13T10:00:00Z",
            ["SPECVERSION"] = "1.0"
        };

        return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(cloudEvent));
    }

    byte[] CreateBase64Payload(byte[] binaryData)
    {
        var cloudEvent = new Dictionary<string, object>
        {
            ["TYPE"] = "com.example.someevent",
            ["SOURCE"] = "/mycontext",
            ["ID"] = Guid.NewGuid().ToString(),
            ["DATA_BASE64"] = Convert.ToBase64String(binaryData),
            ["DATACONTENTTYPE"] = "application/xml",
            ["TIME"] = "2024-01-13T10:00:00Z",
            ["SPECVERSION"] = "1.0"
        };

        return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(cloudEvent));
    }

    byte[] CreateXmlPayload(string xmlData)
    {
        var cloudEvent = new Dictionary<string, object>
        {
            ["TYPE"] = "com.example.someevent",
            ["SOURCE"] = "/mycontext",
            ["ID"] = Guid.NewGuid().ToString(),
            ["DATA"] = xmlData,
            ["DATACONTENTTYPE"] = "application/xml",
            ["TIME"] = "2024-01-13T10:00:00Z",
            ["SPECVERSION"] = "1.0"
        };

        return Encoding.UTF8.GetBytes(JsonSerializer.Serialize(cloudEvent));
    }

    [Benchmark(Description = "Permissive mode - Small JSON payload")]
    public Dictionary<string, string>? PermissiveMode_SmallJson()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        return _permissiveHandler!.UnwrapEnvelope(
            _nativeMessageId!,
            _headers!,
            _smallJsonPayload.AsSpan(),
            new ContextBag(),
            bodyWriter);
    }

    [Benchmark(Description = "Strict mode - Small JSON payload")]
    public Dictionary<string, string>? StrictMode_SmallJson()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        return _strictHandler!.UnwrapEnvelope(
            _nativeMessageId!,
            _headers!,
            _smallJsonPayload.AsSpan(),
            new ContextBag(),
            bodyWriter);
    }

    [Benchmark(Description = "Permissive mode - Medium JSON payload")]
    public Dictionary<string, string>? PermissiveMode_MediumJson()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        return _permissiveHandler!.UnwrapEnvelope(
            _nativeMessageId!,
            _headers!,
            _mediumJsonPayload.AsSpan(),
            new ContextBag(),
            bodyWriter);
    }

    [Benchmark(Description = "Strict mode - Medium JSON payload")]
    public Dictionary<string, string>? StrictMode_MediumJson()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        return _strictHandler!.UnwrapEnvelope(
            _nativeMessageId!,
            _headers!,
            _mediumJsonPayload.AsSpan(),
            new ContextBag(),
            bodyWriter);
    }

    [Benchmark(Description = "Permissive mode - Large JSON payload")]
    public Dictionary<string, string>? PermissiveMode_LargeJson()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        return _permissiveHandler!.UnwrapEnvelope(
            _nativeMessageId!,
            _headers!,
            _largeJsonPayload.AsSpan(),
            new ContextBag(),
            bodyWriter);
    }

    [Benchmark(Description = "Strict mode - Large JSON payload")]
    public Dictionary<string, string>? StrictMode_LargeJson()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        return _strictHandler!.UnwrapEnvelope(
            _nativeMessageId!,
            _headers!,
            _largeJsonPayload.AsSpan(),
            new ContextBag(),
            bodyWriter);
    }

    [Benchmark(Description = "Permissive mode - Base64 binary payload")]
    public Dictionary<string, string>? PermissiveMode_Base64()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        return _permissiveHandler!.UnwrapEnvelope(
            _nativeMessageId!,
            _headers!,
            _base64Payload.AsSpan(),
            new ContextBag(),
            bodyWriter);
    }

    [Benchmark(Description = "Strict mode - Base64 binary payload")]
    public Dictionary<string, string>? StrictMode_Base64()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        return _strictHandler!.UnwrapEnvelope(
            _nativeMessageId!,
            _headers!,
            _base64Payload.AsSpan(),
            new ContextBag(),
            bodyWriter);
    }

    [Benchmark(Description = "Permissive mode - XML payload")]
    public Dictionary<string, string>? PermissiveMode_Xml()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        return _permissiveHandler!.UnwrapEnvelope(
            _nativeMessageId!,
            _headers!,
            _xmlPayload.AsSpan(),
            new ContextBag(),
            bodyWriter);
    }

    [Benchmark(Description = "Strict mode - XML payload", Baseline = true)]
    public Dictionary<string, string>? StrictMode_Xml()
    {
        var bodyWriter = new ArrayBufferWriter<byte>();
        return _strictHandler!.UnwrapEnvelope(
            _nativeMessageId!,
            _headers!,
            _xmlPayload.AsSpan(),
            new ContextBag(),
            bodyWriter);
    }
}