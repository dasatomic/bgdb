``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.388 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.301
  [Host]     : .NET Core 3.1.5 (CoreCLR 4.700.20.26901, CoreFX 4.700.20.27001), X64 RyuJIT
  DefaultJob : .NET Core 3.1.5 (CoreCLR 4.700.20.26901, CoreFX 4.700.20.27001), X64 RyuJIT


```
|          Method | RowsInTableNumber |        Mean |     Error |    StdDev |      Median |
|---------------- |------------------ |------------:|----------:|----------:|------------:|
| **InsertIntoTable** |              **1000** |    **218.3 ms** |   **5.90 ms** |  **17.39 ms** |    **213.6 ms** |
| **InsertIntoTable** |             **10000** | **36,676.1 ms** | **684.90 ms** | **640.66 ms** | **36,828.1 ms** |
