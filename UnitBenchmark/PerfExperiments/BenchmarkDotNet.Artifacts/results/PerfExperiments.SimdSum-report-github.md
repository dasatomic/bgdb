``` ini

BenchmarkDotNet=v0.13.1, OS=Windows 10.0.19043.1415 (21H1/May2021Update)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET SDK=6.0.101
  [Host]     : .NET 6.0.1 (6.0.121.56705), X64 RyuJIT
  Job-FWIMZJ : .NET 6.0.1 (6.0.121.56705), X64 RyuJIT

InvocationCount=1  UnrollFactor=1  

```
|        Method |  ItemNum |            Mean |         Error |        StdDev |         Median |
|-------------- |--------- |----------------:|--------------:|--------------:|---------------:|
|      **NaiveSum** |      **100** |       **134.00 ns** |      **20.58 ns** |      **60.67 ns** |       **100.0 ns** |
|   NaiveSumInt |      100 |        71.91 ns |      16.31 ns |      45.20 ns |       100.0 ns |
|     VectorSum |      100 |        79.52 ns |      15.21 ns |      40.60 ns |       100.0 ns |
|    Intrinsics |      100 |        77.78 ns |      19.82 ns |      58.13 ns |       100.0 ns |
| IntrinsicsInt |      100 |        80.00 ns |      14.43 ns |      40.22 ns |       100.0 ns |
|      **NaiveSum** |     **1000** |       **431.03 ns** |      **23.84 ns** |      **65.26 ns** |       **400.0 ns** |
|   NaiveSumInt |     1000 |       380.61 ns |      18.85 ns |      54.98 ns |       400.0 ns |
|     VectorSum |     1000 |       277.53 ns |      15.15 ns |      41.98 ns |       300.0 ns |
|    Intrinsics |     1000 |       262.89 ns |      19.47 ns |      56.49 ns |       300.0 ns |
| IntrinsicsInt |     1000 |       136.17 ns |      17.69 ns |      50.48 ns |       100.0 ns |
|      **NaiveSum** |   **100000** |    **36,107.69 ns** |     **310.98 ns** |     **259.68 ns** |    **36,100.0 ns** |
|   NaiveSumInt |   100000 |    35,916.67 ns |      91.93 ns |      71.77 ns |    35,900.0 ns |
|     VectorSum |   100000 |    22,587.10 ns |     501.04 ns |   1,421.36 ns |    22,200.0 ns |
|    Intrinsics |   100000 |    17,138.46 ns |     414.79 ns |   1,163.12 ns |    16,800.0 ns |
| IntrinsicsInt |   100000 |     7,907.87 ns |     230.77 ns |     639.46 ns |     7,600.0 ns |
|      **NaiveSum** |  **1000000** |   **750,662.00 ns** |  **22,445.22 ns** |  **66,180.24 ns** |   **756,600.0 ns** |
|   NaiveSumInt |  1000000 |   440,271.11 ns |   8,700.22 ns |  16,553.06 ns |   440,200.0 ns |
|     VectorSum |  1000000 |   677,253.12 ns |  20,858.98 ns |  60,182.94 ns |   674,050.0 ns |
|    Intrinsics |  1000000 |   635,924.49 ns |  20,296.74 ns |  59,206.55 ns |   638,800.0 ns |
| IntrinsicsInt |  1000000 |   305,287.00 ns |   9,270.91 ns |  27,335.48 ns |   312,700.0 ns |
|      **NaiveSum** | **10000000** | **5,363,800.00 ns** | **101,348.33 ns** |  **79,126.11 ns** | **5,376,250.0 ns** |
|   NaiveSumInt | 10000000 | 4,007,873.68 ns |  78,742.73 ns |  87,522.34 ns | 3,964,800.0 ns |
|     VectorSum | 10000000 | 4,611,626.00 ns | 124,039.84 ns | 365,734.26 ns | 4,440,600.0 ns |
|    Intrinsics | 10000000 | 4,253,493.00 ns | 104,953.45 ns | 309,457.60 ns | 4,070,400.0 ns |
| IntrinsicsInt | 10000000 | 2,199,654.55 ns |  55,759.99 ns | 163,534.41 ns | 2,135,200.0 ns |
