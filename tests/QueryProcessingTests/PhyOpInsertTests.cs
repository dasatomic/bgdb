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

            var columnTypes = new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.StringPointer), new ColumnInfo(ColumnType.Double) };
            await using ITransaction tran = logManager.CreateTransaction(allocator);
            int id = await tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnTypes,
                ClusteredIndexPositions = new int[] { }
            }, tran);

            await tran.Commit();

            await using ITransaction tranCreate = logManager.CreateTransaction(allocator);
            var table = await tm.GetById(id, tranCreate);

            var rhf = new RowHolder(new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.Double), new ColumnInfo(ColumnType.String, 10) });
            rhf.SetField<int>(0, 1);
            rhf.SetField(1, 1.ToString().ToCharArray());
            rhf.SetField<double>(2, 1 + 1.1);
            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(rhf);

            await using ITransaction tranInsert = logManager.CreateTransaction(allocator);
            PhyOpTableInsert op = new PhyOpTableInsert(table.Collection, opStatic);
            await op.Iterate(tranInsert).AllResultsAsync();
        }
    }
}