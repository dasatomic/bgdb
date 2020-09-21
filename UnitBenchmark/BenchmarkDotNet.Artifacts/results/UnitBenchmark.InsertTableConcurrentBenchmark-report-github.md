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
| **InsertIntoTableConcurrent** |              **8000** |           **2** |   **381.8 ms** |   **402.97 ms** |  **22.09 ms** |
| **InsertIntoTableConcurrent** |              **8000** |           **4** |   **403.8 ms** |   **247.32 ms** |  **13.56 ms** |
| **InsertIntoTableConcurrent** |              **8000** |           **8** |   **472.1 ms** |   **214.10 ms** |  **11.74 ms** |
| **InsertIntoTableConcurrent** |              **8000** |          **16** |   **559.8 ms** |   **440.32 ms** |  **24.14 ms** |
| **InsertIntoTableConcurrent** |             **16000** |           **2** |   **838.1 ms** |   **290.71 ms** |  **15.93 ms** |
| **InsertIntoTableConcurrent** |             **16000** |           **4** |   **815.2 ms** |   **636.29 ms** |  **34.88 ms** |
| **InsertIntoTableConcurrent** |             **16000** |           **8** |   **856.1 ms** |    **71.41 ms** |   **3.91 ms** |
| **InsertIntoTableConcurrent** |             **16000** |          **16** | **1,074.8 ms** |    **87.02 ms** |   **4.77 ms** |
| **InsertIntoTableConcurrent** |             **32000** |           **2** | **1,658.5 ms** | **2,725.95 ms** | **149.42 ms** |
| **InsertIntoTableConcurrent** |             **32000** |           **4** | **1,605.6 ms** |   **339.67 ms** |  **18.62 ms** |
| **InsertIntoTableConcurrent** |             **32000** |           **8** | **1,768.7 ms** |   **986.37 ms** |  **54.07 ms** |
| **InsertIntoTableConcurrent** |             **32000** |          **16** | **1,903.4 ms** | **1,900.68 ms** | **104.18 ms** |
