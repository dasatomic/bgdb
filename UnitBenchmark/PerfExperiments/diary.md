Starting with long measurements

|    Method | ItemNum |            Mean |         Error |        StdDev |         Median |
|---------- |-------- |----------------:|--------------:|--------------:|---------------:|
|  NaiveSum |     100 |        84.95 ns |      12.67 ns |      35.95 ns |       100.0 ns |
|   LinqSum |     100 |     2,169.79 ns |      95.96 ns |     276.87 ns |     2,100.0 ns |
| VectorSum |     100 |        72.62 ns |      16.70 ns |      44.86 ns |       100.0 ns |
|  NaiveSum |    1000 |       446.88 ns |      21.89 ns |      63.17 ns |       400.0 ns |
|   LinqSum |    1000 |     8,826.32 ns |     174.46 ns |     193.91 ns |     8,800.0 ns |
| VectorSum |    1000 |       276.84 ns |      22.42 ns |      64.33 ns |       300.0 ns |
|  NaiveSum |  100000 |    36,637.50 ns |     712.05 ns |     925.87 ns |    36,200.0 ns |
|   LinqSum |  100000 |   755,141.67 ns |   8,742.81 ns |   6,825.81 ns |   753,250.0 ns |
| VectorSum |  100000 |    23,005.38 ns |   1,187.87 ns |   3,369.79 ns |    21,600.0 ns |
|  NaiveSum | 1000000 |   725,492.55 ns |  16,969.80 ns |  48,415.80 ns |   723,300.0 ns |
|   LinqSum | 1000000 | 4,620,065.91 ns | 113,800.63 ns | 313,439.98 ns | 4,706,950.0 ns |
| VectorSum | 1000000 |   632,921.88 ns |  16,063.48 ns |  46,346.83 ns |   635,550.0 ns |


parallel count for long is 4.

- [x] Understand what parallel count is.

For now it doesn't seem that vector is providing much value at least not compared to pure sum.

Also, it is interesting that this is all well below 1ms. So 1M sum is < 1ms.

|     Method |   ItemNum |             Mean |            Error |           StdDev |          Median |
|----------- |---------- |-----------------:|-----------------:|-----------------:|----------------:|
|   NaiveSum |       100 |         85.56 ns |        12.681 ns |        35.351 ns |        100.0 ns |
|  VectorSum |       100 |        142.86 ns |        22.014 ns |        64.216 ns |        100.0 ns |
| Intrinsics |       100 |        100.00 ns |         0.000 ns |         0.000 ns |        100.0 ns |
|   NaiveSum |      1000 |        384.21 ns |        14.421 ns |        36.707 ns |        400.0 ns |
|  VectorSum |      1000 |        291.36 ns |        10.732 ns |        28.273 ns |        300.0 ns |
| Intrinsics |      1000 |        300.00 ns |         0.000 ns |         0.000 ns |        300.0 ns |
|   NaiveSum |    100000 |     36,066.67 ns |       310.557 ns |       242.462 ns |     36,000.0 ns |
|  VectorSum |    100000 |     21,038.37 ns |       462.755 ns |     1,258.960 ns |     21,400.0 ns |
| Intrinsics |    100000 |     15,976.47 ns |       323.423 ns |       522.267 ns |     16,200.0 ns |
|   NaiveSum |   1000000 |    715,952.81 ns |    15,776.795 ns |    43,717.424 ns |    717,100.0 ns |
|  VectorSum |   1000000 |    632,353.26 ns |    12,924.944 ns |    36,455.035 ns |    635,950.0 ns |
| Intrinsics |   1000000 |    616,650.00 ns |    12,165.369 ns |    20,984.663 ns |    614,250.0 ns |
|   NaiveSum | 100000000 | 59,280,030.61 ns | 1,826,978.729 ns | 5,329,382.705 ns | 58,779,900.0 ns |
|  VectorSum | 100000000 | 44,970,254.00 ns | 1,205,260.638 ns | 3,553,738.184 ns | 42,771,000.0 ns |
| Intrinsics | 100000000 | 42,602,139.00 ns | 1,128,095.514 ns | 3,326,215.076 ns | 41,719,550.0 ns |

Still, improvement is pretty small (20%)...

Nice thing is that we can do 100M in 40ms.

Let's measure on Ints as well:

|        Method |  ItemNum |            Mean |         Error |        StdDev |         Median |
|-------------- |--------- |----------------:|--------------:|--------------:|---------------:|
|      NaiveSum |      100 |       134.00 ns |      20.58 ns |      60.67 ns |       100.0 ns |
|   NaiveSumInt |      100 |        71.91 ns |      16.31 ns |      45.20 ns |       100.0 ns |
|     VectorSum |      100 |        79.52 ns |      15.21 ns |      40.60 ns |       100.0 ns |
|    Intrinsics |      100 |        77.78 ns |      19.82 ns |      58.13 ns |       100.0 ns |
| IntrinsicsInt |      100 |        80.00 ns |      14.43 ns |      40.22 ns |       100.0 ns |
|      NaiveSum |     1000 |       431.03 ns |      23.84 ns |      65.26 ns |       400.0 ns |
|   NaiveSumInt |     1000 |       380.61 ns |      18.85 ns |      54.98 ns |       400.0 ns |
|     VectorSum |     1000 |       277.53 ns |      15.15 ns |      41.98 ns |       300.0 ns |
|    Intrinsics |     1000 |       262.89 ns |      19.47 ns |      56.49 ns |       300.0 ns |
| IntrinsicsInt |     1000 |       136.17 ns |      17.69 ns |      50.48 ns |       100.0 ns |
|      NaiveSum |   100000 |    36,107.69 ns |     310.98 ns |     259.68 ns |    36,100.0 ns |
|   NaiveSumInt |   100000 |    35,916.67 ns |      91.93 ns |      71.77 ns |    35,900.0 ns |
|     VectorSum |   100000 |    22,587.10 ns |     501.04 ns |   1,421.36 ns |    22,200.0 ns |
|    Intrinsics |   100000 |    17,138.46 ns |     414.79 ns |   1,163.12 ns |    16,800.0 ns |
| IntrinsicsInt |   100000 |     7,907.87 ns |     230.77 ns |     639.46 ns |     7,600.0 ns |
|      NaiveSum |  1000000 |   750,662.00 ns |  22,445.22 ns |  66,180.24 ns |   756,600.0 ns |
|   NaiveSumInt |  1000000 |   440,271.11 ns |   8,700.22 ns |  16,553.06 ns |   440,200.0 ns |
|     VectorSum |  1000000 |   677,253.12 ns |  20,858.98 ns |  60,182.94 ns |   674,050.0 ns |
|    Intrinsics |  1000000 |   635,924.49 ns |  20,296.74 ns |  59,206.55 ns |   638,800.0 ns |
| IntrinsicsInt |  1000000 |   305,287.00 ns |   9,270.91 ns |  27,335.48 ns |   312,700.0 ns |
|      NaiveSum | 10000000 | 5,363,800.00 ns | 101,348.33 ns |  79,126.11 ns | 5,376,250.0 ns |
|   NaiveSumInt | 10000000 | 4,007,873.68 ns |  78,742.73 ns |  87,522.34 ns | 3,964,800.0 ns |
|     VectorSum | 10000000 | 4,611,626.00 ns | 124,039.84 ns | 365,734.26 ns | 4,440,600.0 ns |
|    Intrinsics | 10000000 | 4,253,493.00 ns | 104,953.45 ns | 309,457.60 ns | 4,070,400.0 ns |
| IntrinsicsInt | 10000000 | 2,199,654.55 ns |  55,759.99 ns | 163,534.41 ns | 2,135,200.0 ns |


On int on 10M there is 2.5X improvement. Again, I expected more than this.
Again, this on x1600 let's try something else...