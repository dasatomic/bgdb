``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.19041.508 (2004/?/20H1)
AMD Ryzen 5 1600X, 1 CPU, 12 logical and 6 physical cores
.NET Core SDK=3.1.401
  [Host]   : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT
  ShortRun : .NET Core 3.1.7 (CoreCLR 4.700.20.36602, CoreFX 4.700.20.37001), X64 RyuJIT

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
|      Method | TableNumber |        Mean |     Error |   StdDev |
|------------ |------------ |------------:|----------:|---------:|
| **CreateTable** |         **100** |    **82.90 ms** | **295.52 ms** | **16.20 ms** |
| **CreateTable** |        **1000** | **4,173.23 ms** | **938.56 ms** | **51.45 ms** |
