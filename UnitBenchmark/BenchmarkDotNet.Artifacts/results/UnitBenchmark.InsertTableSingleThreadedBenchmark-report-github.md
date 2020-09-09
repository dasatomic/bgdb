``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.450 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |        Mean |        Error |    StdDev |
|---------------- |------------------ |------------:|-------------:|----------:|
| **InsertIntoTable** |               **100** |    **11.55 ms** |     **7.745 ms** |  **0.425 ms** |
| **InsertIntoTable** |              **1000** |   **166.68 ms** |   **232.582 ms** | **12.749 ms** |
| **InsertIntoTable** |              **2000** |   **537.73 ms** |   **658.621 ms** | **36.101 ms** |
| **InsertIntoTable** |              **3000** | **1,183.69 ms** |   **348.140 ms** | **19.083 ms** |
| **InsertIntoTable** |              **4000** | **2,252.40 ms** |   **759.855 ms** | **41.650 ms** |
| **InsertIntoTable** |              **5000** | **3,892.36 ms** | **1,297.338 ms** | **71.111 ms** |
