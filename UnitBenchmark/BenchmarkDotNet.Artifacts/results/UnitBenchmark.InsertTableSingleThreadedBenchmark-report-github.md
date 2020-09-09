``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.450 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |       Mean |     Error |   StdDev |
|---------------- |------------------ |-----------:|----------:|---------:|
| **InsertIntoTable** |              **1000** |   **120.8 ms** |  **87.75 ms** |  **4.81 ms** |
| **InsertIntoTable** |              **2000** |   **201.1 ms** |  **68.59 ms** |  **3.76 ms** |
| **InsertIntoTable** |              **4000** |   **510.3 ms** |  **98.72 ms** |  **5.41 ms** |
| **InsertIntoTable** |              **8000** | **1,412.9 ms** | **575.98 ms** | **31.57 ms** |
| **InsertIntoTable** |             **16000** | **4,459.2 ms** | **776.95 ms** | **42.59 ms** |
