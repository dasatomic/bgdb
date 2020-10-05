# bgDB

`bgDB` is a tiny relational database that aims to make end to end database development more approachable.
The starting point is that database development is fun and that it can help us learn many fundamental computer science concepts. To name a few:
1) Parser/Lexer/Compiler development
2) Data Structures
3) Asyncronous programming
4) Concurrency
5) Performance
6) Working with hardware

`bgDB` doesn't aim to be the fastest nor to provide features that other RDBMSs already provide for decades. It main goal is to be approachable and easily extendable. Language of choice is `C#` for most of the engine and `F#` for Parser and Lexer. Even though managed languages are not the best choice for Database development, given that garbage collection can induce unwanted performance problems and that for many parts developer would want more control, for what we aim to do here they serve the purpose.

# Getting started.

## Dependencies
After cloning the enlistment you will need `dotnet core 3.1` or above. It can be found [here](https://dotnet.microsoft.com/download/dotnet-core/3.1).

## Read eval print loop

To try it out you can start bgdbRepl (read-eval-print-loop) by starting project in bgdbRepl folder. From root enlistment do folloowing:

```cmd
cd bgdbRepl
dotnet run
```

```
Booted Page Manager with file name repl.db. File size 10MB. Creating db file from scratch.
Booted Log Manager. Using in memory stream.
Booted Metadata Manager.
Query entry gate ready.
Transactions are implicit for now. Every command will be executed in separate transaction.
====================
>CREATE TABLE T1 (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(20) c)


Total rows returned 0
```

Inserting rows
```
>INSERT INTO T1 VALUES (1, 1.1, 'somerandomstring1')
Total rows returned 0
>INSERT INTO T1 VALUES (2, 2.2, 'somerandomstring1')
Total rows returned 0
>INSERT INTO T1 VALUES (3, 2.2, 'somerandomstring2')
Total rows returned 0
>INSERT INTO T1 VALUES (5, 14.2, 'somerandomstring2')
Total rows returned 0
```

Querying
```
SELECT MAX(a), MIN(b), c FROM T1 WHERE a > 1 GROUP BY c

|     a_Max |     b_Min |                     c |
-------------------------------------------------
|         2 |       2.2 |     somerandomstring1 |
-------------------------------------------------
|         5 |       2.2 |     somerandomstring2 |
-------------------------------------------------

Total rows returned 2
```

To experiment with slightly larger datasets you can also load titanic dataset by passing set_load_path argument

```
dotnet run --set_load_path .\datasets\titanic-passengers.csv
```

```
>SELECT PassengerId, Survived, Class, Name, Sex, Age, Siblings, Parents, EmbarkedPort FROM Passengers

|PassengerId |Survived |     Class |                                                                Name |     Sex |       Age |  Siblings |   Parents |EmbarkedPort |
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
|       343 |   No     |         2 |                                               CollanderMrErikGustaf |    male |       2:8 |         0 |         0 |           S |
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
|        76 |   No     |         3 |                                                  MoenMrSigurdHansen |    male |       2:5 |         0 |         0 |           S |
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
|       641 |   No    |         3 |                                                    JensenMrHansPeder |    male |       2:0 |         0 |         0 |           S |
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
|       568 |   No    |         3 |                                   PalssonMrsNilsAlmaCorneliaBerglund |  female |       2:9 |         0 |         4 |           S |
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
|       672 |   No    |         1 |                                                   DavidsonMrThornton |    male |       3:1 |         1 |         0 |           S |
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
|       105 |   No    |         3 |                                            GustafssonMrAndersVilhelm |    male |       3:7 |         2 |         0 |           S |
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
|       576 |   No    |         3 |                                                     PatchettMrGeorge |    male |       1:9 |         0 |         0 |           S |
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
|       382 |  Yes    |         3 |                                                   NakidMissMariaMary |  female |       0:1 |         0 |         2 |           C |
----------------------------------------------------------------------------------------------------------------------------------------------------------------------

... (truncating) ...
Total rows returned 711
```
Or something a bit more complex.
```
>SELECT COUNT(PassengerId), Sex, Survived, MIN(Age), MAX(Age) FROM Passengers WHERE EmbarkedPort = 'S' AND Class = 3 GROUP BY Sex, Survived

|PassengerId_Count |     Sex |Survived |   Age_Min |   Age_Max |
----------------------------------------------------------------
|       183        |    male |   No    |      1    |        74 |
----------------------------------------------------------------
|        45        |  female |   No    |      2    |        48 |
----------------------------------------------------------------
|        31        |  female |  Yes    |      1    |        63 |
----------------------------------------------------------------
|        30        |    male |  Yes    |      1    |        45 |
----------------------------------------------------------------

Total rows returned 4
```

If you want to play with more data you can start repl with argument `--rep_load_count N` that will duplicate input dataset N times. E.g.
```
dotnet run --set_load_path .\datasets\titanic-passengers.csv --rep_load_count 1000
```

Will load ~800k rows.

## From code init

Repl is currently pretty limited. There is also no support for transactions in parser layer (transactions are implicit and linked to single command, `BEGIN/COMMIT/ROLLBACK TRAN` support will be added). To get a feeling how things are working under the hood it is best to take a look at end to end tests.

For example:
```cs
[Test]
public async Task MultiTableGroupBy()
{
    await using (ITransaction tran = this.logManager.CreateTransaction(pageManager))
    {
        await this.queryEntryGate.Execute("CREATE TABLE T1 (TYPE_INT A, TYPE_INT B)", tran).AllResultsAsync();
        await this.queryEntryGate.Execute("CREATE TABLE T2 (TYPE_INT A, TYPE_INT B)", tran).AllResultsAsync();
        await this.queryEntryGate.Execute("INSERT INTO T1 VALUES (1, 1)", tran).AllResultsAsync();
        await this.queryEntryGate.Execute("INSERT INTO T1 VALUES (1, 2)", tran).AllResultsAsync();
        await this.queryEntryGate.Execute("INSERT INTO T2 VALUES (2, 3)", tran).AllResultsAsync();
        await this.queryEntryGate.Execute("INSERT INTO T2 VALUES (2, 4)", tran).AllResultsAsync();
        await tran.Commit();
    }

    await using (ITransaction tran = this.logManager.CreateTransaction(pageManager, isReadOnly: true, "SELECT"))
    {
        RowHolder[] result = await this.queryEntryGate.Execute("SELECT MAX(B), A FROM T1 GROUP BY A", tran).ToArrayAsync();
        Assert.AreEqual(1, result.Length);
        Assert.AreEqual(2, result[0].GetField<int>(0));
        Assert.AreEqual(1, result[0].GetField<int>(1));
        result = await this.queryEntryGate.Execute("SELECT MAX(B), A FROM T2 GROUP BY A", tran).ToArrayAsync();
        Assert.AreEqual(1, result.Length);
        Assert.AreEqual(4, result[0].GetField<int>(0));
        Assert.AreEqual(2, result[0].GetField<int>(1));
    }
}
```

E2E tests can be found [here](https://gitlab.com/aleksandartomic88/bgdb/-/tree/master/tests/E2EQueryExecutionTests).

# Supported features and project ramp-up
At this point list of features is rather limited but, hopefully, the list will keep growing.

## Langauge features
1) `CREATE TABLE`
2) `INSERT INTO TABLE`
3) FILTERS (`WHERE` statement with basic arithmetic)
4) `GROUP BY` stetement
5) Aggregates (`MAX`, `MIN`, `SUM`)

## Supported types
1) `TYPE_INT` (32bit signed)
2) `TYPE_DOUBLE` (64 bit)
3) `TYPE_STRING(SIZE)` (strings of fixed length)
4) `TYPE_STRING(MAX)` (work in progress, strings of unlimited length)

## Transactions and locking
Engine currently supports page level locking and read committed isolation. On startup a pool of locks is created and page id maps to lock id through simple modular arithmetic. Lock Manager does deadlock detection and rollbacks deadlock victims.

Logging is traditional [Write Ahead Logging](https://en.wikipedia.org/wiki/Write-ahead_logging). To ramp-up, recommendation is to take a look at `PageManager` [constructor](https://gitlab.com/aleksandartomic88/bgdb/-/blob/master/PageManager/PageManager.cs).

```cs
public PageManager(uint defaultPageSize, IPersistedStream persistedStream, IBufferPool bufferPool, ILockManager lockManager, InstrumentationInterface logger)
```

Log Manager only sits on top of .NET stream that is used for logging.
```cs
public LogManager(BinaryWriter storage)
```

[Lock Manager](https://gitlab.com/aleksandartomic88/bgdb/-/blob/master/LockManager/LockImplementation/AsyncReadWriterLock.cs) currently supports only Read and Write locks that prioritize Writers.

## Data Structures
There is still no support for indexes so tables are currently organized as simple [linked list](https://gitlab.com/aleksandartomic88/bgdb/-/blob/master/DataStructures/PageListCollection.cs). This is hopefully going to change soon.

## Query Processing
Query tree is currently assembled through a set of rules that can be found [here](https://gitlab.com/aleksandartomic88/bgdb/-/blob/master/QueryProcessing/AstToOpTreeBuilder.cs). When work on Query Optimizer starts this will have to change.

Operators follow this simple interface:
```cs
public interface IPhysicalOperator<T>
{
    IAsyncEnumerable<T> Iterate(ITransaction tran);
}
```

Caller gets root operator and keeps draining iterators that are placed deeper in the tree. On the leaf nodes there is `Scan` operator (or, in future, `Seek`, when support for indexes comes).
Operators work on [RowHolder](https://gitlab.com/aleksandartomic88/bgdb/-/blob/master/PageManager/RowHolder.cs) instances which represent single `Row` fetched from Storage Engine.

## Parser/Lexer
Currently we use F# and `FsLexYacc` [library](https://github.com/fsprojects/FsLexYacc). Grammar can be easily understood by looking at [type definitions](https://gitlab.com/aleksandartomic88/bgdb/-/blob/master/ParserLexerFSharp/Sql.fs) and [Parser](https://gitlab.com/aleksandartomic88/bgdb/-/blob/master/ParserLexerFSharp/SqlParser.fsp).

# Testing and Benchmarks
Tests are written using `NUNIT` framework. You can run them either through Visual Studio or simply running `dotnet test` from root folder.

For Benchmarking we use excellent [BenchmarkDotNet](https://github.com/dotnet/BenchmarkDotNet). Benchmarks and results can be found [here](https://gitlab.com/aleksandartomic88/bgdb/-/tree/master/UnitBenchmark).

There are also simple [stress tests](https://gitlab.com/aleksandartomic88/bgdb/-/blob/master/tests/E2EQueryExecutionTests/ConcurrentInsertWithEviction.cs) in E2E folder.

Project also has set up Continuous Integration [pipeline](https://gitlab.com/aleksandartomic88/bgdb/-/pipelines) on GitLab that is running unit tests on every push.

You can build docker container by using provided docker [file](https://gitlab.com/aleksandartomic88/bgdb/-/blob/master/Dockerfile).
