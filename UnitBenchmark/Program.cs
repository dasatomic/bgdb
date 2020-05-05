using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using MetadataManager;
using PageManager;

namespace UnitBenchmark
{
    [MemoryDiagnoser]
    public class MetadataBenchmarks
    {
        [Benchmark(Baseline = true)]
        public void CreateTable()
        {
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);

            var tm = mm.GetTableManager();
            const int repCount = 1000;

            for (int i = 1; i < repCount; i++)
            {
                tm.CreateObject(new TableCreateDefinition()
                {
                    TableName = "T" + i,
                    ColumnNames = new[] { "a", "b", "c" },
                    ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
                });
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<MetadataBenchmarks>();
        }
    }
}
