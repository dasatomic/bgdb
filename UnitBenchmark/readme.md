To generate plots on Windows please install R.
After installation R needs to be added to the Path env and R_HOME var needs to be added.


To run benchmark:
dotnet run -c Release -- --job short --filter *InsertTableSingleThreadedBenchmark*

* Perf results
![Alt text](BenchmarkDotNet.Artifacts/results/UnitBenchmark.InsertTableSingleThreadedBenchmark-barplot.png?raw=true "Insert into table, singlethreaded.")
![Alt text](BenchmarkDotNet.Artifacts/results/UnitBenchmark.RowsetHolderPerf-barplot.png?raw=true "Insert into RowSetHolder.")
![Alt text](BenchmarkDotNet.Artifacts/results/UnitBenchmark.InsertTableConcurrentBenchmark-barplot.png?raw=true "Insert into table, multithreaded.")
![Alt text](BenchmarkDotNet.Artifacts/results/UnitBenchmark.WhereClauseBenchmark-barplot.png?raw=true "Where statement, singlethreaded.")
