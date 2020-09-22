``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|                    Method | RowsInTableNumber | WorkerCount |       Mean |       Error |    StdDev |
|-------------------------- |------------------ |------------ |-----------:|------------:|----------:|
| **InsertIntoTableConcurrent** |              **8000** |           **2** |   **329.2 ms** |   **104.34 ms** |   **5.72 ms** |
| **InsertIntoTableConcurrent** |              **8000** |           **4** |   **361.9 ms** |   **178.31 ms** |   **9.77 ms** |
| **InsertIntoTableConcurrent** |              **8000** |           **8** |   **400.7 ms** |    **84.28 ms** |   **4.62 ms** |
| **InsertIntoTableConcurrent** |              **8000** |          **16** |   **481.6 ms** |   **229.73 ms** |  **12.59 ms** |
| **InsertIntoTableConcurrent** |             **16000** |           **2** |   **662.1 ms** |   **338.03 ms** |  **18.53 ms** |
| **InsertIntoTableConcurrent** |             **16000** |           **4** |   **765.2 ms** |   **480.61 ms** |  **26.34 ms** |
| **InsertIntoTableConcurrent** |             **16000** |           **8** |   **906.4 ms** | **1,079.88 ms** |  **59.19 ms** |
| **InsertIntoTableConcurrent** |             **16000** |          **16** |   **949.2 ms** |   **784.26 ms** |  **42.99 ms** |
| **InsertIntoTableConcurrent** |             **32000** |           **2** | **1,620.8 ms** | **1,294.96 ms** |  **70.98 ms** |
| **InsertIntoTableConcurrent** |             **32000** |           **4** | **1,699.7 ms** | **1,228.53 ms** |  **67.34 ms** |
| **InsertIntoTableConcurrent** |             **32000** |           **8** | **1,663.1 ms** |   **411.82 ms** |  **22.57 ms** |
| **InsertIntoTableConcurrent** |             **32000** |          **16** | **2,006.0 ms** | **2,017.84 ms** | **110.60 ms** |
