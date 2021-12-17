``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19043
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=6.0.100
  [Host]     : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT
  Job-VRFBZG : .NET Core 6.0.0 (CoreCLR 6.0.21.52210, CoreFX 6.0.21.52210), X64 RyuJIT

InvocationCount=1  UnrollFactor=1  

```
|                                                 Method | RowsInTableNumber |       Mean |    Error |   StdDev |
|------------------------------------------------------- |------------------ |-----------:|---------:|---------:|
| **InsertIntoBTreeSingleIntColumnRandomDataBulkSingleTran** |            **100000** |   **368.8 ms** |  **7.09 ms** |  **9.46 ms** |
|  InsertIntoBTreeSingleIntColumnRandomDataTranPerInsert |            100000 |   807.7 ms |  5.11 ms |  4.78 ms |
|      InsertIntoBTreeSingleIntColumnRandomDataDummyTran |            100000 |   199.5 ms |  3.95 ms |  3.50 ms |
| **InsertIntoBTreeSingleIntColumnRandomDataBulkSingleTran** |            **200000** |   **813.8 ms** |  **7.23 ms** |  **6.77 ms** |
|  InsertIntoBTreeSingleIntColumnRandomDataTranPerInsert |            200000 | 1,979.6 ms | 37.46 ms | 69.44 ms |
|      InsertIntoBTreeSingleIntColumnRandomDataDummyTran |            200000 |   491.1 ms |  8.51 ms | 10.46 ms |
| **InsertIntoBTreeSingleIntColumnRandomDataBulkSingleTran** |            **500000** | **2,329.6 ms** | **22.86 ms** | **17.84 ms** |
|  InsertIntoBTreeSingleIntColumnRandomDataTranPerInsert |            500000 | 5,442.3 ms | 63.18 ms | 59.10 ms |
|      InsertIntoBTreeSingleIntColumnRandomDataDummyTran |            500000 | 1,395.3 ms |  9.22 ms |  8.17 ms |
