``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|                    Method | RowsInTableNumber | WorkerCount |    Mean |    Error |   StdDev |
|-------------------------- |------------------ |------------ |--------:|---------:|---------:|
| **InsertIntoTableConcurrent** |              **8000** |           **2** | **1.239 s** | **0.2495 s** | **0.0137 s** |
| **InsertIntoTableConcurrent** |              **8000** |           **4** | **1.303 s** | **0.0741 s** | **0.0041 s** |
| **InsertIntoTableConcurrent** |              **8000** |           **8** | **1.350 s** | **0.1613 s** | **0.0088 s** |
| **InsertIntoTableConcurrent** |              **8000** |          **16** | **1.403 s** | **0.7317 s** | **0.0401 s** |
| **InsertIntoTableConcurrent** |             **16000** |           **2** | **4.084 s** | **0.2057 s** | **0.0113 s** |
| **InsertIntoTableConcurrent** |             **16000** |           **4** | **4.103 s** | **0.2897 s** | **0.0159 s** |
| **InsertIntoTableConcurrent** |             **16000** |           **8** | **4.256 s** | **0.5814 s** | **0.0319 s** |
| **InsertIntoTableConcurrent** |             **16000** |          **16** | **4.501 s** | **1.2548 s** | **0.0688 s** |
