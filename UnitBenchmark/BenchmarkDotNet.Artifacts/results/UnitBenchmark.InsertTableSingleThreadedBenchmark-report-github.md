``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.450 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |        Mean |      Error |    StdDev |
|---------------- |------------------ |------------:|-----------:|----------:|
| **InsertIntoTable** |               **100** |    **10.19 ms** |   **2.437 ms** |  **0.134 ms** |
| **InsertIntoTable** |              **1000** |   **165.57 ms** |  **22.107 ms** |  **1.212 ms** |
| **InsertIntoTable** |              **2000** |   **502.01 ms** | **180.313 ms** |  **9.884 ms** |
| **InsertIntoTable** |              **3000** | **1,153.93 ms** | **114.712 ms** |  **6.288 ms** |
| **InsertIntoTable** |              **4000** | **2,241.63 ms** | **891.628 ms** | **48.873 ms** |
| **InsertIntoTable** |              **5000** | **3,791.60 ms** | **401.607 ms** | **22.013 ms** |
