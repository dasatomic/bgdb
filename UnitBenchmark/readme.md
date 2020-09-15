To generate plots on Windows please install R.
After installation R needs to be added to the Path env and R_HOME var needs to be added.


To run benchmark:
dotnet run -c Release -- --job short --filter *InsertTableSingleThreadedBenchmark*

* Perf results
![Alt text](BenchmarkDotNet.Artifacts/results/UnitBenchmark.InsertTableSingleThreadedBenchmark-barplot.png?raw=true "Insert into table Table, single threaded.")
![Alt text](BenchmarkDotNet.Artifacts/results/UnitBenchmark.RowsetHolderPerf-barplot.png?raw=true "Insert into RowSetHolder.")
