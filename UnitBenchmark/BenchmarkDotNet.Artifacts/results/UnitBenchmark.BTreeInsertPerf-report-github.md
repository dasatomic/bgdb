``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19043
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=6.0.100
  [Host]     : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT
  DefaultJob : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT


```
|                                   Method | RowsInTableNumber |        Mean |     Error |    StdDev |
|----------------------------------------- |------------------ |------------:|----------:|----------:|
| **InsertIntoBTreeSingleIntColumnRandomData** |             **10000** |    **69.08 ms** |  **0.896 ms** |  **0.839 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |             **50000** |   **402.94 ms** |  **1.798 ms** |  **1.502 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **100000** |   **893.23 ms** |  **8.578 ms** |  **7.163 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **200000** | **2,063.97 ms** | **31.334 ms** | **26.166 ms** |
