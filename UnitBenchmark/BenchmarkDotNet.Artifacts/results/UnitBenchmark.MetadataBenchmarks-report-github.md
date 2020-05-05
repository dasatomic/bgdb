``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.18363.778 (1909/November2018Update/19H2)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.201
  [Host]     : .NET Core 3.1.3 (CoreCLR 4.700.20.11803, CoreFX 4.700.20.12001), X64 RyuJIT
  DefaultJob : .NET Core 3.1.3 (CoreCLR 4.700.20.11803, CoreFX 4.700.20.12001), X64 RyuJIT


```
|      Method |    Mean |    Error |   StdDev | Ratio |       Gen 0 |     Gen 1 |     Gen 2 | Allocated |
|------------ |--------:|---------:|---------:|------:|------------:|----------:|----------:|----------:|
| CreateTable | 2.125 s | 0.0136 s | 0.0121 s |  1.00 | 103000.0000 | 2000.0000 | 1000.0000 | 424.96 MB |
