``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |      Mean |      Error |    StdDev |
|---------------- |------------------ |----------:|-----------:|----------:|
| **InsertIntoTable** |              **1000** |  **42.42 ms** |   **9.817 ms** |  **0.538 ms** |
| **InsertIntoTable** |              **2000** | **129.98 ms** | **386.550 ms** | **21.188 ms** |
| **InsertIntoTable** |              **4000** | **178.88 ms** | **198.090 ms** | **10.858 ms** |
| **InsertIntoTable** |              **8000** | **354.50 ms** | **273.213 ms** | **14.976 ms** |
| **InsertIntoTable** |             **16000** | **661.28 ms** |  **82.349 ms** |  **4.514 ms** |
