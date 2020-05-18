using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.IO;

namespace QueryProcessingTests
{
    public class PhyOpInsertTests
    {
        [Test]
        public void ValidateInsert()
        {
            var allocator = new InMemoryPageManager(4096);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = new Transaction(logManager, allocator, "SETUP");
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();

            var columnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double };
            ITransaction tran = new Transaction(logManager, allocator, "CREATE_TABLE_TEST");
            int id = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnTypes,
            }, tran);

            tran.Commit();

            tran = new Transaction(logManager, allocator, "GET_TABLE");
            var table = tm.GetById(id, tran);

            Row[] source = new Row[] { new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }, columnTypes) };
            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            tran = new Transaction(logManager, allocator, "INSERT");
            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic, tran);
            op.Invoke();
            tran.Commit();
        }
    }
}