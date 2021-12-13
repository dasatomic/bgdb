``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19043
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=6.0.100
  [Host]     : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT
  DefaultJob : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT


```
|                                   Method | RowsInTableNumber |       Mean |    Error |   StdDev |
|----------------------------------------- |------------------ |-----------:|---------:|---------:|
| **InsertIntoBTreeSingleIntColumnRandomData** |            **100000** |   **204.4 ms** |  **4.03 ms** |  **4.14 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **200000** |   **466.1 ms** |  **3.73 ms** |  **3.31 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **500000** | **1,374.4 ms** | **26.82 ms** | **44.81 ms** |
