``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|          Method | RowsInTableNumber |      Mean |      Error |    StdDev |
|---------------- |------------------ |----------:|-----------:|----------:|
| **InsertIntoTable** |            **100000** |  **49.91 ms** |   **5.811 ms** |  **0.319 ms** |
| **InsertIntoTable** |            **200000** | **142.35 ms** |  **63.825 ms** |  **3.498 ms** |
| **InsertIntoTable** |            **500000** | **366.09 ms** | **125.709 ms** |  **6.891 ms** |
| **InsertIntoTable** |           **1000000** | **704.91 ms** | **202.359 ms** | **11.092 ms** |
