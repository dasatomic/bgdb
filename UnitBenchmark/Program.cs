using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using DataStructures;
using LogManager;
using MetadataManager;
using PageManager;
using QueryProcessing;
using System.IO;
using Test.Common;

namespace UnitBenchmark
{
    public class Benchmarks
    {
        [Params(10, 100, 1000)]
        public int TableNumber;

        [Params(10, 100, 1000, 10000)]
        public int RowsInTableNumber;

        [Benchmark]
        public void CreateTable()
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = logManager.CreateTransaction(allocator, "SETUP");

            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();

            for (int i = 1; i < TableNumber; i++)
            {
                ITransaction tran = logManager.CreateTransaction(allocator, "CT");
                tm.CreateObject(new TableCreateDefinition()
                {
                    TableName = "T" + i,
                    ColumnNames = new[] { "a", "b", "c" },
                    ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
                }, tran).Wait();
            }
        }

        [Benchmark]
        public void InsertIntoTable()
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = logManager.CreateTransaction(allocator, "SETUP");

            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);
            var tm = mm.GetTableManager();

            var columnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double };
            ITransaction tran = logManager.CreateTransaction(allocator, "CT");
            int id = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnTypes,
            }, tran).Result;

            ITransaction tranFT = logManager.CreateTransaction(allocator, "FETCH_TABLE");
            var table = tm.GetById(id, tranFT).Result;
            tran.Commit().Wait();

            Row[] source = new Row[] { new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }, columnTypes) };

            for (int i = 0; i < RowsInTableNumber; i++)
            {
                PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

                tran = logManager.CreateTransaction(allocator, "INSERT");
                PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic, tran);
                op.Invoke().Wait();
                tran.Commit().Wait();
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<Benchmarks>();
        }
    }
}
