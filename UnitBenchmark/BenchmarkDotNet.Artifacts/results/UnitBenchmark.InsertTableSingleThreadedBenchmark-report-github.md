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
| **InsertIntoTable** |              **1000** |   **115.3 ms** |  **65.56 ms** |  **3.59 ms** |
| **InsertIntoTable** |              **2000** |   **204.7 ms** |  **78.07 ms** |  **4.28 ms** |
| **InsertIntoTable** |              **4000** |   **529.7 ms** | **153.76 ms** |  **8.43 ms** |
| **InsertIntoTable** |              **8000** | **1,594.2 ms** |  **89.87 ms** |  **4.93 ms** |
| **InsertIntoTable** |             **16000** | **5,663.5 ms** | **486.67 ms** | **26.68 ms** |
