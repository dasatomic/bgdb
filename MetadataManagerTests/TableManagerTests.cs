using NUnit.Framework;
using MetadataManager;
using PageManager;
using System.Linq;
using LogManager;
using System.IO;
using Test.Common;
using DataStructures;
using System.Threading.Tasks;

namespace MetadataManagerTests
{
    public class TableManagerTests
    {
        [Test]
        public async Task CreateTable()
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));

            using ITransaction setupTran = logManager.CreateTransaction(allocator);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);
            await setupTran.Commit();

            ITransaction tran = logManager.CreateTransaction(allocator);

            var tm = mm.GetTableManager();
            int objId = await tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "A",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
            }, tran);

            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);

            Assert.True(await tm.Exists(new TableCreateDefinition()
            {
                TableName = "A",
            }, tran));

            MetadataTable table = await tm.GetById(objId, tran);
            Assert.AreEqual("A", table.TableName);

            Assert.AreEqual(new[] { "a", "b", "c" }, table.Columns.Select(t => t.ColumnName));
            Assert.AreEqual(new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }, table.Columns.Select(c => c.ColumnType));

            var cm = mm.GetColumnManager();

            tran = logManager.CreateTransaction(allocator);
            await foreach (var c in cm.Iterate(tran))
            {
                Assert.Contains(c.ColumnName.ToString(), new[] { "a", "b", "c" });
            }
        }

        [Test]
        public async Task CreateMultiTable()
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = logManager.CreateTransaction(allocator);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();
            const int repCount = 100;

            for (int i = 1; i < repCount; i++)
            {
                ITransaction tran = logManager.CreateTransaction(allocator);
                int id = await tm.CreateObject(new TableCreateDefinition()
                {
                    TableName = "T" + i,
                    ColumnNames = new[] { "a", "b", "c" },
                    ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
                }, tran);
                await tran.Commit();
            }

            for (int i = 1; i < repCount; i++)
            {
                ITransaction tran = logManager.CreateTransaction(allocator);
                var table = await tm.GetById(i, tran);
                Assert.AreEqual("T" + i, table.TableName);

                Assert.AreEqual(new[] { "a", "b", "c" }, table.Columns.Select(c => c.ColumnName));
                Assert.AreEqual(new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }, table.Columns.Select(c => c.ColumnType));
            }
        }

        [Test]
        public async Task CreateWithSameName()
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = logManager.CreateTransaction(allocator);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();
            ITransaction tran = logManager.CreateTransaction(allocator);
            int objId = await tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "A",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
            }, tran);
            await tran.Commit();

            Assert.ThrowsAsync<ElementWithSameNameExistsException>(async () =>
            {
                ITransaction tran = logManager.CreateTransaction(allocator);
                await tm.CreateObject(new TableCreateDefinition()
                {
                    TableName = "A",
                    ColumnNames = new[] { "a" },
                    ColumnTypes = new[] { ColumnType.Int }
                }, tran);
            });
        }
    }
}
