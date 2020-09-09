``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.450 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |       Mean |       Error |   StdDev |
|---------------- |------------------ |-----------:|------------:|---------:|
| **InsertIntoTable** |              **1000** |   **112.3 ms** |   **262.57 ms** | **14.39 ms** |
| **InsertIntoTable** |              **2000** |   **215.5 ms** |   **161.54 ms** |  **8.85 ms** |
| **InsertIntoTable** |              **4000** |   **513.9 ms** |    **66.25 ms** |  **3.63 ms** |
| **InsertIntoTable** |              **8000** | **1,435.6 ms** |   **292.05 ms** | **16.01 ms** |
| **InsertIntoTable** |             **16000** | **4,701.2 ms** | **1,312.20 ms** | **71.93 ms** |
