``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19043
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=6.0.100
  [Host]     : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT
  Job-BRNTFY : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT

InvocationCount=1  UnrollFactor=1  

```
|                                   Method | RowsInTableNumber |       Mean |    Error |   StdDev |
|----------------------------------------- |------------------ |-----------:|---------:|---------:|
| **InsertIntoBTreeSingleIntColumnRandomData** |            **100000** |   **198.3 ms** |  **2.01 ms** |  **1.68 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **200000** |   **478.0 ms** |  **3.87 ms** |  **3.62 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **500000** | **1,422.7 ms** | **13.80 ms** | **11.53 ms** |
