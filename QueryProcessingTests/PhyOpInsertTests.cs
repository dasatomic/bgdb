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

            int id = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
            });

            var table = tm.GetById(id);

            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap);

            Row r = new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" });
            op.Invoke(r);
        }
    }
}