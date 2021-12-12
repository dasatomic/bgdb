``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19043
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=6.0.100
  [Host]     : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT
  DefaultJob : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT


```
|                                   Method | RowsInTableNumber |     Mean |   Error |  StdDev |
|----------------------------------------- |------------------ |---------:|--------:|--------:|
| **InsertIntoBTreeSingleIntColumnRandomData** |             **50000** | **134.7 ms** | **2.15 ms** | **2.21 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **100000** | **315.2 ms** | **5.77 ms** | **5.12 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **200000** | **868.2 ms** | **3.62 ms** | **3.39 ms** |
