using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Diagnostics.Windows.Configs;
using BenchmarkDotNet.Running;
using DataStructures;
using LogManager;
using PageManager;
using QueryProcessing;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Test.Common;

namespace UnitBenchmark
{
    public static class BenchmarkUtils
    {
        public static async Task<(ILogManager, IPageManager, QueryEntryGate)> GetLogAndQueryEntryGate()
        {
            var pageManager =  new PageManager.PageManager(4096, new LruEvictionPolicy(1000, 5), TestGlobals.DefaultPersistedStream);
            var logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            StringHeapCollection stringHeap = null;

            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "SETUP"))
            {
                stringHeap = new StringHeapCollection(pageManager, tran);
                await tran.Commit();
            }

            var metadataManager = new MetadataManager.MetadataManager(pageManager, stringHeap, pageManager, logManager);
            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(metadataManager);

            var queryEntryGate = new QueryEntryGate(
                statementHandlers: new ISqlStatement[]
                {
                    new CreateTableStatement(metadataManager),
                    new InsertIntoTableStatement(treeBuilder),
                    new SelectStatement(treeBuilder),
                });

            return (logManager, pageManager, queryEntryGate);
        }
    }

    [RPlotExporter]
    [EtwProfiler(performExtraBenchmarksRun: false)]
    public class InsertTableSingleThreadedBenchmark
    {
        [Params(10000, 20000, 40000, 100000)]
        public int RowsInTableNumber;

        [Benchmark]
        public async Task InsertIntoTable()
        {
            (ILogManager logManager, IPageManager pageManager, QueryEntryGate queryEntryGate) = await BenchmarkUtils.GetLogAndQueryEntryGate();
            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            for (int i = 0; i < this.RowsInTableNumber; i++)
            {
                await using (ITransaction tran = logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string insertQuery = "INSERT INTO Table VALUES (1, 1.1, 'mystring')";
                    await queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                    await tran.Commit();
                }
            }
        }
    }

    [RPlotExporter]
    public class InsertTableConcurrentBenchmark
    {
        [Params(8000, 16000, 32000)]
        public int RowsInTableNumber;

        [Params(2, 4, 8, 16)]
        public int WorkerCount;

        [Benchmark]
        public async Task InsertIntoTableConcurrent()
        {
            (ILogManager logManager, IPageManager pageManager, QueryEntryGate queryEntryGate) = await BenchmarkUtils.GetLogAndQueryEntryGate();
            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING(10) c)";
                await queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            async Task insertAction()
            {
                    for (int i = 1; i <= RowsInTableNumber / WorkerCount; i++)
                    {
                        using (ITransaction tran = logManager.CreateTransaction(pageManager, "GET_ROWS"))
                        {
                            string insertQuery = "INSERT INTO Table VALUES (1, 1.1, 'mystring')";
                            await queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                            await tran.Commit();
                        }
                }
            }

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < WorkerCount; i++)
            {
                tasks.Add(Task.Run(insertAction));
            }

            await Task.WhenAll(tasks);
        }
    }

    class Program
    {
        static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
