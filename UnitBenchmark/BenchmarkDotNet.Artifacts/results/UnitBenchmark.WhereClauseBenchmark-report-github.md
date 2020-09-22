``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |      Mean |     Error |    StdDev |
|---------------- |------------------ |----------:|----------:|----------:|
| **InsertIntoTable** |            **100000** |  **52.12 ms** |  **36.15 ms** |  **1.981 ms** |
| **InsertIntoTable** |            **200000** | **137.37 ms** |  **43.63 ms** |  **2.392 ms** |
| **InsertIntoTable** |            **500000** | **320.38 ms** |  **25.81 ms** |  **1.415 ms** |
| **InsertIntoTable** |           **1000000** | **646.85 ms** | **253.78 ms** | **13.910 ms** |
