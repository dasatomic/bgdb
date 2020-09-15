``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |       Mean |       Error |    StdDev |
|---------------- |------------------ |-----------:|------------:|----------:|
| **InsertIntoTable** |              **1000** |   **110.3 ms** |   **297.16 ms** |  **16.29 ms** |
| **InsertIntoTable** |              **2000** |   **185.3 ms** |    **99.34 ms** |   **5.45 ms** |
| **InsertIntoTable** |              **4000** |   **438.6 ms** |    **67.61 ms** |   **3.71 ms** |
| **InsertIntoTable** |              **8000** | **1,219.7 ms** |   **225.25 ms** |  **12.35 ms** |
| **InsertIntoTable** |             **16000** | **4,075.4 ms** | **2,026.03 ms** | **111.05 ms** |
