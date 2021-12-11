``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19043
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=6.0.100
  [Host]     : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT
  DefaultJob : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT


```
|                                   Method | RowsInTableNumber |       Mean |    Error |   StdDev |
|----------------------------------------- |------------------ |-----------:|---------:|---------:|
| **InsertIntoBTreeSingleIntColumnRandomData** |             **50000** |   **346.4 ms** |  **5.63 ms** |  **4.39 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **100000** |   **748.1 ms** | **11.31 ms** | **10.03 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **200000** | **1,676.4 ms** | **24.03 ms** | **18.76 ms** |
