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
| **InsertIntoTable** |            **100000** |  **49.14 ms** |   **6.252 ms** |  **0.343 ms** |
| **InsertIntoTable** |            **200000** | **143.72 ms** |  **48.925 ms** |  **2.682 ms** |
| **InsertIntoTable** |            **500000** | **367.63 ms** | **398.483 ms** | **21.842 ms** |
| **InsertIntoTable** |           **1000000** | **703.96 ms** | **382.272 ms** | **20.954 ms** |
