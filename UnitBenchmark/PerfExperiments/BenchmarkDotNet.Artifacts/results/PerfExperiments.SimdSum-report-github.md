``` ini

BenchmarkDotNet=v0.13.1, OS=Windows 10.0.19043.1415 (21H1/May2021Update)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET SDK=6.0.101
  [Host]   : .NET 6.0.1 (6.0.121.56705), X64 RyuJIT
  .NET 6.0 : .NET 6.0.1 (6.0.121.56705), X64 RyuJIT

Job=.NET 6.0  Runtime=.NET 6.0  InvocationCount=1  
UnrollFactor=1  

```
|                  Method |  ItemNum |            Mean |        Error |        StdDev |          Median | Ratio | RatioSD |
|------------------------ |--------- |----------------:|-------------:|--------------:|----------------:|------:|--------:|
|            **NaiveSumLong** |     **8000** |      **2,968.8 ns** |     **48.74 ns** |      **47.87 ns** |      **3,000.0 ns** |  **1.00** |    **0.02** |
|             NaiveSumInt |     8000 |      2,943.5 ns |     62.29 ns |      78.78 ns |      2,900.0 ns |  1.00 |    0.00 |
|             LinqSumLong |     8000 |     61,530.8 ns |    294.90 ns |     246.25 ns |     61,500.0 ns | 20.79 |    0.47 |
|              LinqSumInt |     8000 |     61,523.1 ns |    321.16 ns |     268.18 ns |     61,500.0 ns | 20.78 |    0.43 |
|           VectorSumLong |     8000 |      1,639.5 ns |     55.61 ns |     151.29 ns |      1,700.0 ns |  0.55 |    0.06 |
|            VectorSumInt |     8000 |        832.3 ns |     29.48 ns |      83.62 ns |        900.0 ns |  0.28 |    0.03 |
|       IntrinsicsSumLong |     8000 |      1,355.4 ns |     31.10 ns |      83.01 ns |      1,400.0 ns |  0.46 |    0.03 |
|           IntrinsicsInt |     8000 |        739.8 ns |     23.90 ns |      67.79 ns |        700.0 ns |  0.25 |    0.03 |
|    IntrinsicsIntAligned |     8000 |        672.2 ns |     22.19 ns |      61.86 ns |        700.0 ns |  0.22 |    0.02 |
| IntrinsicsIntLoopUnfold |     8000 |        642.9 ns |     20.69 ns |      58.01 ns |        600.0 ns |  0.22 |    0.02 |
|                         |          |                 |              |               |                 |       |         |
|            **NaiveSumLong** |    **10000** |      **3,707.7 ns** |     **76.70 ns** |      **64.05 ns** |      **3,700.0 ns** |  **1.01** |    **0.03** |
|             NaiveSumInt |    10000 |      3,673.3 ns |     75.23 ns |      70.37 ns |      3,700.0 ns |  1.00 |    0.00 |
|             LinqSumLong |    10000 |     76,584.6 ns |    659.27 ns |     550.52 ns |     76,400.0 ns | 20.88 |    0.47 |
|              LinqSumInt |    10000 |     76,664.3 ns |    252.66 ns |     223.98 ns |     76,700.0 ns | 20.89 |    0.42 |
|           VectorSumLong |    10000 |      2,014.4 ns |     71.58 ns |     207.66 ns |      2,100.0 ns |  0.57 |    0.04 |
|            VectorSumInt |    10000 |      1,041.1 ns |     43.58 ns |     125.05 ns |      1,100.0 ns |  0.28 |    0.04 |
|       IntrinsicsSumLong |    10000 |      1,680.9 ns |     46.62 ns |     133.02 ns |      1,700.0 ns |  0.46 |    0.03 |
|           IntrinsicsInt |    10000 |        957.4 ns |     26.83 ns |      76.54 ns |        950.0 ns |  0.27 |    0.02 |
|    IntrinsicsIntAligned |    10000 |        838.9 ns |     21.97 ns |      61.25 ns |        800.0 ns |  0.23 |    0.01 |
| IntrinsicsIntLoopUnfold |    10000 |        754.7 ns |     20.82 ns |      59.73 ns |        700.0 ns |  0.21 |    0.02 |
|                         |          |                 |              |               |                 |       |         |
|            **NaiveSumLong** |   **100000** |     **36,235.7 ns** |    **571.12 ns** |     **506.28 ns** |     **36,000.0 ns** |  **1.01** |    **0.02** |
|             NaiveSumInt |   100000 |     36,041.7 ns |    275.57 ns |     215.15 ns |     36,000.0 ns |  1.00 |    0.00 |
|             LinqSumLong |   100000 |    752,685.7 ns |  1,917.89 ns |   1,700.16 ns |    753,200.0 ns | 20.89 |    0.12 |
|              LinqSumInt |   100000 |              NA |           NA |            NA |              NA |     ? |       ? |
|           VectorSumLong |   100000 |     17,823.1 ns |    492.06 ns |   1,379.78 ns |     17,900.0 ns |  0.49 |    0.04 |
|            VectorSumInt |   100000 |      9,194.6 ns |    410.43 ns |   1,157.62 ns |      9,200.0 ns |  0.24 |    0.02 |
|       IntrinsicsSumLong |   100000 |     19,179.6 ns |    978.18 ns |   2,853.39 ns |     18,750.0 ns |  0.54 |    0.09 |
|           IntrinsicsInt |   100000 |      8,592.6 ns |    413.55 ns |   1,179.87 ns |      8,350.0 ns |  0.24 |    0.03 |
|    IntrinsicsIntAligned |   100000 |      8,056.5 ns |    316.26 ns |     892.00 ns |      7,750.0 ns |  0.23 |    0.03 |
| IntrinsicsIntLoopUnfold |   100000 |              NA |           NA |            NA |              NA |     ? |       ? |
|                         |          |                 |              |               |                 |       |         |
|            **NaiveSumLong** |  **1000000** |    **773,884.7 ns** | **21,603.99 ns** |  **63,019.86 ns** |    **798,050.0 ns** |  **1.70** |    **0.17** |
|             NaiveSumInt |  1000000 |    455,775.8 ns | 10,957.14 ns |  30,725.00 ns |    451,700.0 ns |  1.00 |    0.00 |
|             LinqSumLong |  1000000 |  4,328,846.1 ns | 97,170.70 ns | 269,259.56 ns |  4,270,800.0 ns |  9.53 |    0.92 |
|              LinqSumInt |  1000000 |              NA |           NA |            NA |              NA |     ? |       ? |
|           VectorSumLong |  1000000 |    657,319.4 ns | 19,995.60 ns |  58,328.11 ns |    672,550.0 ns |  1.45 |    0.15 |
|            VectorSumInt |  1000000 |    315,421.9 ns |  9,567.91 ns |  27,605.62 ns |    314,650.0 ns |  0.70 |    0.08 |
|       IntrinsicsSumLong |  1000000 |    638,812.0 ns | 19,935.76 ns |  58,781.05 ns |    654,300.0 ns |  1.40 |    0.16 |
|           IntrinsicsInt |  1000000 |    304,032.7 ns | 10,703.94 ns |  31,223.90 ns |    302,200.0 ns |  0.67 |    0.09 |
|    IntrinsicsIntAligned |  1000000 |    301,453.5 ns |  9,863.19 ns |  28,927.02 ns |    299,600.0 ns |  0.66 |    0.08 |
| IntrinsicsIntLoopUnfold |  1000000 |    292,272.7 ns |  8,450.70 ns |  24,784.43 ns |    294,200.0 ns |  0.64 |    0.07 |
|                         |          |                 |              |               |                 |       |         |
|            **NaiveSumLong** | **10000000** |  **5,354,478.9 ns** | **99,199.57 ns** | **110,260.05 ns** |  **5,343,200.0 ns** |  **1.35** |    **0.03** |
|             NaiveSumInt | 10000000 |  3,948,358.3 ns | 50,021.50 ns |  39,053.49 ns |  3,936,450.0 ns |  1.00 |    0.00 |
|             LinqSumLong | 10000000 | 42,487,961.5 ns | 54,753.78 ns |  45,721.89 ns | 42,493,800.0 ns | 10.76 |    0.11 |
|              LinqSumInt | 10000000 |              NA |           NA |            NA |              NA |     ? |       ? |
|           VectorSumLong | 10000000 |  4,032,097.2 ns | 79,478.01 ns | 132,789.90 ns |  3,991,050.0 ns |  1.03 |    0.04 |
|            VectorSumInt | 10000000 |  2,066,835.0 ns | 41,003.76 ns |  47,219.99 ns |  2,066,200.0 ns |  0.52 |    0.01 |
|       IntrinsicsSumLong | 10000000 |  3,900,585.7 ns | 51,143.67 ns |  45,337.54 ns |  3,891,050.0 ns |  0.99 |    0.01 |
|           IntrinsicsInt | 10000000 |  2,020,721.1 ns | 37,865.03 ns |  42,086.88 ns |  2,030,000.0 ns |  0.51 |    0.01 |
|    IntrinsicsIntAligned | 10000000 |  2,043,932.7 ns | 39,462.22 ns |  81,496.32 ns |  2,024,550.0 ns |  0.52 |    0.02 |
| IntrinsicsIntLoopUnfold | 10000000 |  1,991,828.0 ns | 39,609.40 ns |  52,877.41 ns |  1,995,600.0 ns |  0.50 |    0.02 |

Benchmarks with issues:
  SimdSum.LinqSumInt: .NET 6.0(Runtime=.NET 6.0, InvocationCount=1, UnrollFactor=1) [ItemNum=100000]
  SimdSum.IntrinsicsIntLoopUnfold: .NET 6.0(Runtime=.NET 6.0, InvocationCount=1, UnrollFactor=1) [ItemNum=100000]
  SimdSum.LinqSumInt: .NET 6.0(Runtime=.NET 6.0, InvocationCount=1, UnrollFactor=1) [ItemNum=1000000]
  SimdSum.LinqSumInt: .NET 6.0(Runtime=.NET 6.0, InvocationCount=1, UnrollFactor=1) [ItemNum=10000000]
