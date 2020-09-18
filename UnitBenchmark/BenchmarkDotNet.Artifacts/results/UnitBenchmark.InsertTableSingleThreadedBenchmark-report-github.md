``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |       Mean |       Error |   StdDev |
|---------------- |------------------ |-----------:|------------:|---------:|
| **InsertIntoTable** |             **10000** |   **428.9 ms** |    **75.70 ms** |  **4.15 ms** |
| **InsertIntoTable** |             **20000** |   **840.5 ms** |   **184.56 ms** | **10.12 ms** |
| **InsertIntoTable** |             **40000** | **1,778.4 ms** |   **535.67 ms** | **29.36 ms** |
| **InsertIntoTable** |            **100000** | **4,230.1 ms** | **1,025.72 ms** | **56.22 ms** |
