``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|                Method |  IterNum |       Mean |      Error |    StdDev |
|---------------------- |--------- |-----------:|-----------:|----------:|
| **RowsetHolderFixedTest** |   **100000** |   **2.859 ms** |  **0.8365 ms** | **0.0459 ms** |
| **RowsetHolderFixedTest** |   **500000** |  **13.939 ms** |  **5.5525 ms** | **0.3044 ms** |
| **RowsetHolderFixedTest** |  **1000000** |  **27.536 ms** |  **4.6452 ms** | **0.2546 ms** |
| **RowsetHolderFixedTest** | **10000000** | **296.535 ms** | **58.2210 ms** | **3.1913 ms** |
