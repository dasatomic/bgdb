``` ini

BenchmarkDotNet=v0.13.1, OS=Windows 10.0.19043.1415 (21H1/May2021Update)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET SDK=6.0.101
  [Host]   : .NET 6.0.1 (6.0.121.56705), X64 RyuJIT
  .NET 6.0 : .NET 6.0.1 (6.0.121.56705), X64 RyuJIT

Job=.NET 6.0  Runtime=.NET 6.0  InvocationCount=1  
UnrollFactor=1  

```
|                  Method |  ItemNum |            Mean |         Error |        StdDev |          Median | Ratio | RatioSD |
|------------------------ |--------- |----------------:|--------------:|--------------:|----------------:|------:|--------:|
|            **NaiveSumLong** |     **8000** |      **2,921.4 ns** |      **48.03 ns** |      **42.58 ns** |      **2,900.0 ns** |  **0.99** |    **0.04** |
|             NaiveSumInt |     8000 |      2,944.0 ns |      61.47 ns |      82.06 ns |      2,900.0 ns |  1.00 |    0.00 |
|             LinqSumLong |     8000 |     61,607.1 ns |     281.46 ns |     249.51 ns |     61,650.0 ns | 20.95 |    0.66 |
|              LinqSumInt |     8000 |     61,566.7 ns |     199.43 ns |     155.70 ns |     61,600.0 ns | 21.01 |    0.72 |
|           VectorSumLong |     8000 |      1,537.5 ns |      71.20 ns |     196.12 ns |      1,500.0 ns |  0.53 |    0.09 |
|            VectorSumInt |     8000 |        820.2 ns |      33.17 ns |      91.92 ns |        800.0 ns |  0.27 |    0.03 |
|       IntrinsicsSumLong |     8000 |      1,336.5 ns |      32.71 ns |      88.43 ns |      1,300.0 ns |  0.45 |    0.04 |
|           IntrinsicsInt |     8000 |        747.3 ns |      25.68 ns |      72.02 ns |        700.0 ns |  0.26 |    0.02 |
|          IntrinsicsByte |     8000 |        274.4 ns |      16.13 ns |      43.89 ns |        300.0 ns |  0.09 |    0.02 |
|    IntrinsicsIntAligned |     8000 |        685.7 ns |      17.70 ns |      35.36 ns |        700.0 ns |  0.24 |    0.01 |
| IntrinsicsIntLoopUnfold |     8000 |        576.0 ns |      17.01 ns |      43.00 ns |        600.0 ns |  0.20 |    0.02 |
|                         |          |                 |               |               |                 |       |         |
|            **NaiveSumLong** |    **10000** |      **3,666.7 ns** |      **65.98 ns** |      **61.72 ns** |      **3,700.0 ns** |  **1.00** |    **0.03** |
|             NaiveSumInt |    10000 |      3,657.1 ns |      72.90 ns |      64.62 ns |      3,650.0 ns |  1.00 |    0.00 |
|             LinqSumLong |    10000 |     76,691.7 ns |     410.37 ns |     320.39 ns |     76,750.0 ns | 20.97 |    0.40 |
|              LinqSumInt |    10000 |     76,407.1 ns |     352.49 ns |     312.47 ns |     76,300.0 ns | 20.90 |    0.36 |
|           VectorSumLong |    10000 |      1,930.3 ns |      89.96 ns |     249.27 ns |      1,900.0 ns |  0.51 |    0.06 |
|            VectorSumInt |    10000 |      1,077.4 ns |      51.49 ns |     146.06 ns |      1,100.0 ns |  0.29 |    0.05 |
|       IntrinsicsSumLong |    10000 |      1,844.4 ns |     143.04 ns |     376.83 ns |      1,800.0 ns |  0.46 |    0.03 |
|           IntrinsicsInt |    10000 |        926.7 ns |      30.33 ns |      84.53 ns |        900.0 ns |  0.25 |    0.03 |
|          IntrinsicsByte |    10000 |        322.2 ns |      20.67 ns |      57.63 ns |        300.0 ns |  0.09 |    0.02 |
|    IntrinsicsIntAligned |    10000 |        871.4 ns |      21.18 ns |      45.58 ns |        900.0 ns |  0.24 |    0.01 |
| IntrinsicsIntLoopUnfold |    10000 |        766.3 ns |      22.28 ns |      60.61 ns |        800.0 ns |  0.21 |    0.02 |
|                         |          |                 |               |               |                 |       |         |
|            **NaiveSumLong** |   **100000** |     **36,392.3 ns** |     **714.29 ns** |     **596.46 ns** |     **36,100.0 ns** |  **1.01** |    **0.02** |
|             NaiveSumInt |   100000 |     35,966.7 ns |     157.66 ns |     123.09 ns |     35,900.0 ns |  1.00 |    0.00 |
|             LinqSumLong |   100000 |    752,608.3 ns |   2,064.91 ns |   1,612.15 ns |    752,950.0 ns | 20.93 |    0.10 |
|              LinqSumInt |   100000 |              NA |            NA |            NA |              NA |     ? |       ? |
|           VectorSumLong |   100000 |     18,544.6 ns |     910.16 ns |   2,567.14 ns |     17,400.0 ns |  0.51 |    0.05 |
|            VectorSumInt |   100000 |      9,438.2 ns |     511.45 ns |   1,417.22 ns |      9,100.0 ns |  0.27 |    0.05 |
|       IntrinsicsSumLong |   100000 |     17,821.1 ns |     787.27 ns |   2,258.83 ns |     16,900.0 ns |  0.50 |    0.08 |
|           IntrinsicsInt |   100000 |      9,064.9 ns |     539.73 ns |   1,565.85 ns |      9,000.0 ns |  0.24 |    0.04 |
|          IntrinsicsByte |   100000 |      2,164.6 ns |      50.76 ns |     134.60 ns |      2,150.0 ns |  0.06 |    0.01 |
|    IntrinsicsIntAligned |   100000 |      8,264.1 ns |     364.44 ns |   1,027.92 ns |      8,000.0 ns |  0.23 |    0.03 |
| IntrinsicsIntLoopUnfold |   100000 |      7,278.3 ns |     405.30 ns |   1,143.14 ns |      7,150.0 ns |  0.20 |    0.03 |
|                         |          |                 |               |               |                 |       |         |
|            **NaiveSumLong** |  **1000000** |    **720,251.0 ns** |  **24,757.47 ns** |  **72,997.96 ns** |    **721,150.0 ns** |  **1.62** |    **0.18** |
|             NaiveSumInt |  1000000 |    448,515.7 ns |   8,969.55 ns |  23,941.56 ns |    447,500.0 ns |  1.00 |    0.00 |
|             LinqSumLong |  1000000 |  7,592,784.6 ns |  44,381.81 ns |  37,060.82 ns |  7,590,500.0 ns | 16.90 |    0.79 |
|              LinqSumInt |  1000000 |              NA |            NA |            NA |              NA |     ? |       ? |
|           VectorSumLong |  1000000 |    619,774.7 ns |  23,758.19 ns |  69,678.66 ns |    631,100.0 ns |  1.40 |    0.18 |
|            VectorSumInt |  1000000 |    310,196.0 ns |  12,206.95 ns |  35,800.88 ns |    314,000.0 ns |  0.69 |    0.10 |
|       IntrinsicsSumLong |  1000000 |    599,432.0 ns |  22,140.39 ns |  65,281.45 ns |    614,300.0 ns |  1.35 |    0.16 |
|           IntrinsicsInt |  1000000 |    303,661.2 ns |  10,934.35 ns |  31,896.01 ns |    309,100.0 ns |  0.68 |    0.09 |
|          IntrinsicsByte |  1000000 |     60,106.2 ns |   2,718.20 ns |   7,885.98 ns |     59,400.0 ns |  0.13 |    0.02 |
|    IntrinsicsIntAligned |  1000000 |    310,066.3 ns |   9,134.92 ns |  26,646.98 ns |    313,050.0 ns |  0.70 |    0.07 |
| IntrinsicsIntLoopUnfold |  1000000 |    294,453.6 ns |  10,035.36 ns |  29,114.39 ns |    297,600.0 ns |  0.66 |    0.07 |
|                         |          |                 |               |               |                 |       |         |
|            **NaiveSumLong** | **10000000** |  **5,445,800.0 ns** | **107,251.83 ns** |  **89,560.14 ns** |  **5,418,400.0 ns** |  **1.35** |    **0.04** |
|             NaiveSumInt | 10000000 |  4,030,893.3 ns |  78,247.32 ns |  73,192.59 ns |  4,043,200.0 ns |  1.00 |    0.00 |
|             LinqSumLong | 10000000 | 48,709,486.7 ns | 958,170.45 ns | 896,273.23 ns | 48,159,200.0 ns | 12.09 |    0.35 |
|              LinqSumInt | 10000000 |              NA |            NA |            NA |              NA |     ? |       ? |
|           VectorSumLong | 10000000 |  4,076,454.1 ns |  81,403.23 ns | 138,228.87 ns |  4,056,100.0 ns |  1.02 |    0.04 |
|            VectorSumInt | 10000000 |  2,120,653.1 ns |  42,351.91 ns |  65,936.82 ns |  2,104,150.0 ns |  0.53 |    0.02 |
|       IntrinsicsSumLong | 10000000 |  3,903,060.9 ns |  76,265.75 ns |  96,451.75 ns |  3,921,200.0 ns |  0.96 |    0.03 |
|           IntrinsicsInt | 10000000 |  2,133,238.1 ns |  40,582.72 ns | 109,022.85 ns |  2,119,750.0 ns |  0.54 |    0.03 |
|          IntrinsicsByte | 10000000 |    638,852.6 ns |  16,422.03 ns |  47,643.28 ns |    642,900.0 ns |  0.15 |    0.01 |
|    IntrinsicsIntAligned | 10000000 |  2,136,742.9 ns |  56,320.84 ns | 164,290.54 ns |  2,066,450.0 ns |  0.54 |    0.05 |
| IntrinsicsIntLoopUnfold | 10000000 |  2,021,420.0 ns |  39,702.21 ns |  70,570.63 ns |  2,005,500.0 ns |  0.50 |    0.02 |

Benchmarks with issues:
  SimdSum.LinqSumInt: .NET 6.0(Runtime=.NET 6.0, InvocationCount=1, UnrollFactor=1) [ItemNum=100000]
  SimdSum.LinqSumInt: .NET 6.0(Runtime=.NET 6.0, InvocationCount=1, UnrollFactor=1) [ItemNum=1000000]
  SimdSum.LinqSumInt: .NET 6.0(Runtime=.NET 6.0, InvocationCount=1, UnrollFactor=1) [ItemNum=10000000]
