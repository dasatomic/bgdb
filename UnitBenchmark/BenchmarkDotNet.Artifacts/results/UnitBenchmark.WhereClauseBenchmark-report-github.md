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
| **InsertIntoTable** |             **10000** |  **5.087 ms** |  **3.046 ms** | **0.1670 ms** |
| **InsertIntoTable** |             **20000** | **10.260 ms** |  **2.267 ms** | **0.1242 ms** |
| **InsertIntoTable** |             **40000** | **20.021 ms** | **20.999 ms** | **1.1510 ms** |
| **InsertIntoTable** |             **80000** | **38.995 ms** |  **6.517 ms** | **0.3572 ms** |
| **InsertIntoTable** |            **100000** | **50.810 ms** | **32.311 ms** | **1.7711 ms** |
