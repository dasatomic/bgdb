using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;

namespace QueryProcessingTests
{
    public class PhyOpInsertTests
    {
        [Test]
        public void ValidateInsert()
        {
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator);
            var tm = mm.GetTableManager();

            var columnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double };
            int id = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnTypes,
            });

            var table = tm.GetById(id);

            Row[] source = new Row[] { new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }, columnTypes) };
            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic);
            op.Invoke();
        }
    }
}