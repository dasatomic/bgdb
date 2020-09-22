``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |       Mean |      Error |   StdDev |
|---------------- |------------------ |-----------:|-----------:|---------:|
| **InsertIntoTable** |             **10000** |   **461.9 ms** |   **508.9 ms** | **27.89 ms** |
| **InsertIntoTable** |             **20000** |   **817.0 ms** |   **344.0 ms** | **18.86 ms** |
| **InsertIntoTable** |             **40000** | **1,625.6 ms** |   **623.4 ms** | **34.17 ms** |
| **InsertIntoTable** |            **100000** | **4,124.1 ms** | **1,324.1 ms** | **72.58 ms** |
