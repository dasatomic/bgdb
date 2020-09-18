``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|                    Method | RowsInTableNumber | WorkerCount |       Mean |      Error |    StdDev |
|-------------------------- |------------------ |------------ |-----------:|-----------:|----------:|
| **InsertIntoTableConcurrent** |              **8000** |           **2** |   **332.5 ms** |   **168.6 ms** |   **9.24 ms** |
| **InsertIntoTableConcurrent** |              **8000** |           **4** |   **356.5 ms** |   **248.2 ms** |  **13.60 ms** |
| **InsertIntoTableConcurrent** |              **8000** |           **8** |   **395.5 ms** |   **186.5 ms** |  **10.22 ms** |
| **InsertIntoTableConcurrent** |              **8000** |          **16** |   **475.9 ms** |   **534.8 ms** |  **29.31 ms** |
| **InsertIntoTableConcurrent** |             **16000** |           **2** |   **650.3 ms** |   **195.4 ms** |  **10.71 ms** |
| **InsertIntoTableConcurrent** |             **16000** |           **4** |   **685.0 ms** |   **330.5 ms** |  **18.12 ms** |
| **InsertIntoTableConcurrent** |             **16000** |           **8** |   **764.6 ms** |   **355.6 ms** |  **19.49 ms** |
| **InsertIntoTableConcurrent** |             **16000** |          **16** |   **930.2 ms** |   **768.9 ms** |  **42.14 ms** |
| **InsertIntoTableConcurrent** |             **32000** |           **2** | **1,377.7 ms** | **3,015.6 ms** | **165.30 ms** |
| **InsertIntoTableConcurrent** |             **32000** |           **4** | **1,442.1 ms** | **1,022.8 ms** |  **56.06 ms** |
| **InsertIntoTableConcurrent** |             **32000** |           **8** | **1,626.1 ms** | **1,362.6 ms** |  **74.69 ms** |
| **InsertIntoTableConcurrent** |             **32000** |          **16** | **1,865.4 ms** | **1,448.3 ms** |  **79.39 ms** |
