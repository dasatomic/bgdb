``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.22000
Intel Core i5-1035G4 CPU 1.10GHz, 1 CPU, 8 logical and 4 physical cores
.NET Core SDK=6.0.100
  [Host]     : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT
  DefaultJob : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT


```
|                                   Method | RowsInTableNumber |     Mean |    Error |   StdDev |
|----------------------------------------- |------------------ |---------:|---------:|---------:|
| **InsertIntoBTreeSingleIntColumnRandomData** |             **50000** | **154.1 ms** |  **7.68 ms** | **22.65 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **100000** | **311.8 ms** |  **9.21 ms** | **26.72 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **200000** | **679.7 ms** | **17.52 ms** | **49.71 ms** |
