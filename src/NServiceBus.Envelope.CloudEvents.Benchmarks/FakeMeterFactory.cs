namespace NServiceBus.Envelope.CloudEvents.Benchmarks;

using System.Diagnostics.Metrics;

class FakeMeterFactory : IMeterFactory
{
    readonly List<Meter> _meters = [];

    public void Dispose()
    {
        foreach (var meter in _meters)
        {
            meter.Dispose();
        }
    }

    public Meter Create(MeterOptions options)
    {
        var meter = new Meter(options);
        _meters.Add(meter);
        return meter;
    }
}