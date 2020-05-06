using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using MetadataManager;
using PageManager;
using QueryProcessing;

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
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);

            var tm = mm.GetTableManager();

            for (int i = 1; i < TableNumber; i++)
            {
                tm.CreateObject(new TableCreateDefinition()
                {
                    TableName = "T" + i,
                    ColumnNames = new[] { "a", "b", "c" },
                    ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
                });
            }
        }

        [Benchmark]
        public void InsertIntoTable()
        {
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);
            var tm = mm.GetTableManager();

            int id = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
            });

            var table = tm.GetById(id);

            Row[] source = new Row[] { new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }) };

            for (int i = 0; i < RowsInTableNumber; i++)
            {
                PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

                PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic);
                op.Invoke();
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
