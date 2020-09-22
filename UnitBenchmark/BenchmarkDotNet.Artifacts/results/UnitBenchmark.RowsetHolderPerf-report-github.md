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
| **RowsetHolderFixedTest** |   **100000** |   **2.827 ms** |  **0.1190 ms** | **0.0065 ms** |
| **RowsetHolderFixedTest** |   **500000** |  **13.828 ms** |  **6.5694 ms** | **0.3601 ms** |
| **RowsetHolderFixedTest** |  **1000000** |  **27.473 ms** |  **3.3465 ms** | **0.1834 ms** |
| **RowsetHolderFixedTest** | **10000000** | **299.064 ms** | **50.6497 ms** | **2.7763 ms** |
