``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19043
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=6.0.100
  [Host]     : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT
  DefaultJob : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT


```
|                                   Method | RowsInTableNumber |        Mean |     Error |    StdDev |
|----------------------------------------- |------------------ |------------:|----------:|----------:|
| **InsertIntoBTreeSingleIntColumnRandomData** |             **10000** |    **90.98 ms** |  **1.795 ms** |  **3.000 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |             **50000** |   **556.63 ms** |  **6.904 ms** |  **5.391 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **100000** | **1,379.85 ms** | **27.219 ms** | **39.036 ms** |
| **InsertIntoBTreeSingleIntColumnRandomData** |            **200000** | **3,208.92 ms** | **36.132 ms** | **32.030 ms** |
