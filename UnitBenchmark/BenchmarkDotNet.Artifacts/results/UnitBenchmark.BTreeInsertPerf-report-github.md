``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19043
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=6.0.100
  [Host]     : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT
  DefaultJob : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT


```
|                                   Method | RowsInTableNumber |     Mean |   Error |  StdDev |
|----------------------------------------- |------------------ |---------:|--------:|--------:|
| **InsertIntoBTreeSingleIntColumnRandomData** |             **50000** | **128.7 ms** | **0.90 ms** | **0.75 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **100000** | **317.5 ms** | **4.82 ms** | **4.51 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **200000** | **840.5 ms** | **6.95 ms** | **6.50 ms** |
