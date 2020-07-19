using DataStructures;
using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.IO;
using System.Threading.Tasks;
using Test.Common;

namespace QueryProcessingTests
{
    public class PhyOpInsertTests
    {
        [Test]
        public async Task ValidateInsert()
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            await using ITransaction setupTran = logManager.CreateTransaction(allocator);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();

            var columnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double };
            await using ITransaction tran = logManager.CreateTransaction(allocator);
            int id = await tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnTypes,
            }, tran);

            await tran.Commit();

            await using ITransaction tranCreate = logManager.CreateTransaction(allocator);
            var table = await tm.GetById(id, tranCreate);

            Row[] source = new Row[] { new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }, columnTypes) };
            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            await using ITransaction tranInsert = logManager.CreateTransaction(allocator);
            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic, tranInsert);
            await op.Invoke();
        }
    }
}