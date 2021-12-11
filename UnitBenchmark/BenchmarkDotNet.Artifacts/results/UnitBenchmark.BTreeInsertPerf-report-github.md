``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19043
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=6.0.100
  [Host]     : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT
  DefaultJob : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT


```
|                                   Method | RowsInTableNumber |       Mean |   Error |  StdDev |
|----------------------------------------- |------------------ |-----------:|--------:|--------:|
| **InsertIntoBTreeSingleIntColumnRandomData** |             **50000** |   **219.4 ms** | **4.38 ms** | **4.68 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **100000** |   **487.0 ms** | **3.34 ms** | **2.79 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **200000** | **1,200.3 ms** | **5.32 ms** | **4.45 ms** |
