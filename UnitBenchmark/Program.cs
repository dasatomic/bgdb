using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using DataStructures;
using LogManager;
using MetadataManager;
using PageManager;
using QueryProcessing;
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
            var pageManager =  new PageManager.PageManager(4096, new FifoEvictionPolicy(100, 5), TestGlobals.DefaultPersistedStream);
            var logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            StringHeapCollection stringHeap = null;

            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "SETUP"))
            {
                stringHeap = new StringHeapCollection(pageManager, tran);
                await tran.Commit();
            }

            var metadataManager = new MetadataManager.MetadataManager(pageManager, stringHeap, pageManager, logManager);
            AstToOpTreeBuilder treeBuilder = new AstToOpTreeBuilder(metadataManager, stringHeap, pageManager);

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

    public class CreateTableBenchmarks
    {
        [Params(10, 100, 1000)]
        public int TableNumber;

        [Benchmark]
        public async Task CreateTable()
        {
            (ILogManager logManager, IPageManager pageManager, QueryEntryGate queryEntryGate) = await BenchmarkUtils.GetLogAndQueryEntryGate();

            for (int i = 0; i < this.TableNumber; i++)
            {
                await using (ITransaction tran = logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
                {
                    string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
                    await queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                    await tran.Commit();
                }
            }
        }
    }

    public class InsertTableSingleThreaded
    {
        [Params(1000, 10000)]
        public int RowsInTableNumber;

        [Benchmark]
        public async Task InsertIntoTable()
        {
            (ILogManager logManager, IPageManager pageManager, QueryEntryGate queryEntryGate) = await BenchmarkUtils.GetLogAndQueryEntryGate();
            await using (ITransaction tran = logManager.CreateTransaction(pageManager, "CREATE_TABLE"))
            {
                string createTableQuery = "CREATE TABLE Table (TYPE_INT a, TYPE_DOUBLE b, TYPE_STRING c)";
                await queryEntryGate.Execute(createTableQuery, tran).ToArrayAsync();
                await tran.Commit();
            }

            for (int i = 0; i < this.RowsInTableNumber; i++)
            {
                await using (ITransaction tran = logManager.CreateTransaction(pageManager, "INSERT"))
                {
                    string insertQuery = "INSERT INTO Table VALUES (1, 1.1, mystring)";
                    await queryEntryGate.Execute(insertQuery, tran).ToArrayAsync();
                    await tran.Commit();
                }
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<InsertTableSingleThreaded>();
        }
    }
}
