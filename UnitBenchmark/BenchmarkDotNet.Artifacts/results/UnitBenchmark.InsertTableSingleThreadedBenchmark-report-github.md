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
| **InsertIntoTable** |             **10000** |   **421.9 ms** |   **185.43 ms** | **10.16 ms** |
| **InsertIntoTable** |             **20000** |   **817.4 ms** |   **108.04 ms** |  **5.92 ms** |
| **InsertIntoTable** |             **40000** | **1,609.3 ms** |    **98.71 ms** |  **5.41 ms** |
| **InsertIntoTable** |            **100000** | **4,441.5 ms** | **1,137.09 ms** | **62.33 ms** |
