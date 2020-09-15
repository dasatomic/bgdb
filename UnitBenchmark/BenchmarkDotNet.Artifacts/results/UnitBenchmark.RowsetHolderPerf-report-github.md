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
| **RowsetHolderFixedTest** |   **100000** |   **2.603 ms** |  **0.3501 ms** | **0.0192 ms** |
| **RowsetHolderFixedTest** |   **500000** |  **13.508 ms** |  **0.8189 ms** | **0.0449 ms** |
| **RowsetHolderFixedTest** |  **1000000** |  **26.900 ms** |  **0.3168 ms** | **0.0174 ms** |
| **RowsetHolderFixedTest** | **10000000** | **278.508 ms** | **13.2478 ms** | **0.7262 ms** |
