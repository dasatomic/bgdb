``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |       Mean |     Error |   StdDev |
|---------------- |------------------ |-----------:|----------:|---------:|
| **InsertIntoTable** |              **1000** |   **116.1 ms** |  **36.91 ms** |  **2.02 ms** |
| **InsertIntoTable** |              **2000** |   **190.8 ms** |  **33.31 ms** |  **1.83 ms** |
| **InsertIntoTable** |              **4000** |   **493.0 ms** |  **42.29 ms** |  **2.32 ms** |
| **InsertIntoTable** |              **8000** | **1,465.7 ms** | **224.03 ms** | **12.28 ms** |
| **InsertIntoTable** |             **16000** | **4,839.9 ms** | **283.63 ms** | **15.55 ms** |
