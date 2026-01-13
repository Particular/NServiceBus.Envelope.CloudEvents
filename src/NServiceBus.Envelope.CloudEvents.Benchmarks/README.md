# NServiceBus.Envelope.CloudEvents Benchmarks

This project contains performance benchmarks for the CloudEvents envelope unwrapper, specifically for JSON Structured CloudEvents.

## Running the Benchmarks

To run all benchmarks:

```bash
cd src/NServiceBus.Envelope.CloudEvents.Benchmarks
dotnet run -c Release
```

To run specific benchmarks:

```bash
dotnet run -c Release --filter "*SmallJson*"
```

To list all available benchmarks:

```bash
dotnet run -c Release --list flat
```

## Benchmark Scenarios

The benchmarks test the `CloudEventJsonStructuredEnvelopeHandler` with various scenarios:

### Handler Modes
- **Permissive Mode**: More lenient CloudEvents validation
- **Strict Mode**: Strict CloudEvents validation with Content-Type checking

### Payload Types
- **Small JSON**: Simple JSON object with a single property
- **Medium JSON**: Typical business message with order details
- **Large JSON**: Complex message with 100 items and nested properties
- **Base64 Binary**: Binary data encoded as base64
- **XML**: XML payload in the data field

## Benchmark Configuration

- **MemoryDiagnoser**: Enabled to track memory allocations
- **Warmup Count**: 3 iterations
- **Iteration Count**: 10 iterations per benchmark
- **Baseline**: `StrictMode_Xml` is set as the baseline for comparison

## Results

Results are stored in `BenchmarkDotNet.Artifacts` directory after running the benchmarks. Look for:
- `results/` - Contains markdown, HTML, and CSV formatted results
- Detailed performance metrics including:
  - Mean execution time
  - Standard deviation
  - Memory allocations (Gen0, Gen1, Gen2, and total allocated)
  - Comparison ratios to baseline

## Understanding the Results

The benchmarks help identify:
- Performance differences between Strict and Permissive modes
- Impact of payload size on processing time
- Memory allocation patterns
- Efficiency of the buffer writer API