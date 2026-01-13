namespace NServiceBus.Envelope.CloudEvents.Benchmarks;

using BenchmarkDotNet.Running;

static class Program
{
    public static void Main(string[] args)
    {
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}