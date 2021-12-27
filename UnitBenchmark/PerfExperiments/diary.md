|                  Method |  ItemNum |            Mean |         Error |          StdDev |          Median | Ratio | RatioSD |
|------------------------ |--------- |----------------:|--------------:|----------------:|----------------:|------:|--------:|
|            NaiveSumLong |     8000 |      2,952.9 ns |      60.79 ns |        62.43 ns |      2,900.0 ns |  1.01 |    0.03 |
|             NaiveSumInt |     8000 |      2,929.2 ns |      62.02 ns |        80.65 ns |      2,900.0 ns |  1.00 |    0.00 |
|             LinqSumLong |     8000 |     61,278.6 ns |     198.76 ns |       176.19 ns |     61,250.0 ns | 20.84 |    0.59 |
|              LinqSumInt |     8000 |     75,170.0 ns |   4,917.40 ns |    14,499.07 ns |     62,050.0 ns | 25.20 |    4.82 |
|           VectorSumLong |     8000 |      1,682.0 ns |      37.52 ns |       103.97 ns |      1,700.0 ns |  0.56 |    0.04 |
|            VectorSumInt |     8000 |        833.3 ns |      29.55 ns |        83.84 ns |        800.0 ns |  0.30 |    0.04 |
|       IntrinsicsSumLong |     8000 |      1,360.8 ns |      31.06 ns |        80.73 ns |      1,400.0 ns |  0.47 |    0.04 |
|           IntrinsicsInt |     8000 |        737.4 ns |      23.15 ns |        67.89 ns |        700.0 ns |  0.25 |    0.02 |
|    IntrinsicsIntAligned |     8000 |        696.2 ns |       9.40 ns |        19.42 ns |        700.0 ns |  0.24 |    0.01 |
| IntrinsicsIntLoopUnfold |     8000 |        677.6 ns |      18.76 ns |        54.74 ns |        700.0 ns |  0.23 |    0.02 |
|                         |          |                 |               |                 |                 |       |         |
|            NaiveSumLong |    10000 |      3,583.3 ns |      49.86 ns |        38.92 ns |      3,600.0 ns |  1.00 |    0.02 |
|             NaiveSumInt |    10000 |      3,578.6 ns |      48.03 ns |        42.58 ns |      3,600.0 ns |  1.00 |    0.00 |
|             LinqSumLong |    10000 |     76,321.4 ns |     230.67 ns |       204.48 ns |     76,300.0 ns | 21.33 |    0.25 |
|              LinqSumInt |    10000 |     76,453.8 ns |     205.19 ns |       171.34 ns |     76,500.0 ns | 21.33 |    0.23 |
|           VectorSumLong |    10000 |      1,952.6 ns |      70.02 ns |       203.15 ns |      2,000.0 ns |  0.55 |    0.05 |
|            VectorSumInt |    10000 |        980.4 ns |      36.10 ns |       104.72 ns |      1,000.0 ns |  0.28 |    0.03 |
|       IntrinsicsSumLong |    10000 |      1,667.7 ns |      36.24 ns |        82.53 ns |      1,700.0 ns |  0.48 |    0.03 |
|           IntrinsicsInt |    10000 |        884.5 ns |      25.53 ns |        74.08 ns |        900.0 ns |  0.25 |    0.02 |
|    IntrinsicsIntAligned |    10000 |        855.4 ns |      21.01 ns |        52.71 ns |        900.0 ns |  0.24 |    0.02 |
| IntrinsicsIntLoopUnfold |    10000 |        835.4 ns |      24.11 ns |        69.55 ns |        800.0 ns |  0.24 |    0.02 |
|                         |          |                 |               |                 |                 |       |         |
|            NaiveSumLong |   100000 |     36,650.0 ns |     733.32 ns |       844.49 ns |     36,250.0 ns |  1.01 |    0.02 |
|             NaiveSumInt |   100000 |     36,092.9 ns |     200.16 ns |       177.44 ns |     36,050.0 ns |  1.00 |    0.00 |
|             LinqSumLong |   100000 |    752,585.7 ns |   1,959.30 ns |     1,736.87 ns |    752,700.0 ns | 20.85 |    0.10 |
|              LinqSumInt |   100000 |              NA |            NA |              NA |              NA |     ? |       ? |
|           VectorSumLong |   100000 |     18,005.3 ns |     701.07 ns |     2,000.18 ns |     17,850.0 ns |  0.52 |    0.07 |
|            VectorSumInt |   100000 |      9,094.4 ns |     355.67 ns |       991.46 ns |      8,900.0 ns |  0.26 |    0.03 |
|       IntrinsicsSumLong |   100000 |     17,433.3 ns |     527.24 ns |     1,521.20 ns |     17,050.0 ns |  0.49 |    0.05 |
|           IntrinsicsInt |   100000 |      8,001.1 ns |     266.26 ns |       759.67 ns |      7,750.0 ns |  0.22 |    0.03 |
|    IntrinsicsIntAligned |   100000 |      7,905.6 ns |     226.01 ns |       630.03 ns |      7,600.0 ns |  0.22 |    0.02 |
| IntrinsicsIntLoopUnfold |   100000 |      7,240.0 ns |     324.19 ns |       930.16 ns |      6,700.0 ns |  0.20 |    0.02 |
|                         |          |                 |               |                 |                 |       |         |
|            NaiveSumLong |  1000000 |    784,767.3 ns |  16,012.76 ns |    46,709.97 ns |    792,850.0 ns |  1.76 |    0.14 |
|             NaiveSumInt |  1000000 |    446,201.1 ns |   8,727.66 ns |    23,891.83 ns |    443,900.0 ns |  1.00 |    0.00 |
|             LinqSumLong |  1000000 |  7,762,413.3 ns |  92,495.74 ns |    86,520.57 ns |  7,778,300.0 ns | 17.41 |    0.97 |
|              LinqSumInt |  1000000 |              NA |            NA |              NA |              NA |     ? |       ? |
|           VectorSumLong |  1000000 |    680,035.4 ns |  15,976.99 ns |    46,857.73 ns |    693,500.0 ns |  1.53 |    0.13 |
|            VectorSumInt |  1000000 |    308,986.6 ns |   9,594.88 ns |    27,836.48 ns |    304,600.0 ns |  0.70 |    0.08 |
|       IntrinsicsSumLong |  1000000 |    650,190.9 ns |  20,091.81 ns |    58,925.81 ns |    667,700.0 ns |  1.47 |    0.16 |
|           IntrinsicsInt |  1000000 |    308,260.4 ns |   8,235.07 ns |    23,760.06 ns |    310,900.0 ns |  0.69 |    0.07 |
|    IntrinsicsIntAligned |  1000000 |    302,336.8 ns |  10,240.86 ns |    29,382.95 ns |    303,000.0 ns |  0.68 |    0.07 |
| IntrinsicsIntLoopUnfold |  1000000 |    285,735.4 ns |   8,668.44 ns |    25,423.04 ns |    280,400.0 ns |  0.64 |    0.06 |
|                         |          |                 |               |                 |                 |       |         |
|            NaiveSumLong | 10000000 |  5,457,819.0 ns | 108,532.79 ns |   129,200.56 ns |  5,453,100.0 ns |  1.36 |    0.05 |
|             NaiveSumInt | 10000000 |  4,010,833.3 ns |  79,652.18 ns |    74,506.70 ns |  3,995,300.0 ns |  1.00 |    0.00 |
|             LinqSumLong | 10000000 | 43,457,847.7 ns | 861,543.86 ns | 2,343,893.61 ns | 42,850,950.0 ns | 11.41 |    0.35 |
|              LinqSumInt | 10000000 |              NA |            NA |              NA |              NA |     ? |       ? |
|           VectorSumLong | 10000000 |  4,046,892.3 ns |  75,564.80 ns |    63,100.03 ns |  4,037,800.0 ns |  1.01 |    0.02 |
|            VectorSumInt | 10000000 |  2,084,181.2 ns |  41,241.23 ns |    64,207.63 ns |  2,078,950.0 ns |  0.52 |    0.02 |
|       IntrinsicsSumLong | 10000000 |  4,051,023.1 ns |  51,500.20 ns |    43,005.00 ns |  4,055,300.0 ns |  1.01 |    0.02 |
|           IntrinsicsInt | 10000000 |  2,067,927.0 ns |  40,797.78 ns |    69,277.73 ns |  2,058,300.0 ns |  0.52 |    0.02 |
|    IntrinsicsIntAligned | 10000000 |  2,051,221.2 ns |  40,815.61 ns |    64,737.88 ns |  2,036,700.0 ns |  0.51 |    0.02 |
| IntrinsicsIntLoopUnfold | 10000000 |  2,048,394.6 ns |  36,936.36 ns |    62,720.75 ns |  2,051,100.0 ns |  0.52 |    0.02 |

Findings:

1. LINQ is just so slow => 20x slower than simple for loop.
2. SIMD over ints gives 4X-5X improvement if we keep hitting L1/L2 cache. Cpu I am using has 64kbs per core which means that ~8k of ints can fit it. For longs we see ~2x improvement. Avx2 can execute over 256 bits (4 longs, 8 ints) so I expected a bit better results.
3. I expected to see bigger diff between 8k rows and 100k rows since 8k fits fully into L1 cache.
4. Perf is stable < 100k elems. e.g. diff between 10k and 100k is ~10x. But 1M is 40X slower than 100k (for SIMD).
5. For regular loop this degradation is smaller - it's not always linear but slowdown between 100k and 1M is ~13x.
6. Memory becomes the bottleneck if we have larger arrays. SIMD gives 5X improvement < 100k elems. On 1M it is less than 2X.
7. Manual loop unfolding somewhat helps (gives 1-2%). Not worth it.
8. When added benchmark on SIMDs over bytes (32X potential), I get 10X - to even ~20X improvement. It is worthwhile to use the right type.

I wanted to see near 8X improvement but got ~5x. Altough, it may be interesting to measure what we could get if we empoy all CPUs (8 x 5 => aim at 40x).