using NUnit.Framework;
using MetadataManager;
using PageManager;
using System.Linq;
using LogManager;
using System.IO;
using Test.Common;
using DataStructures;

namespace MetadataManagerTests
{
    public class TableManagerTests
    {
        [Test]
        public void CreateTable()
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));

            ITransaction setupTran = new Transaction(logManager, allocator, "SETUP");
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);
            setupTran.Commit();

            ITransaction tran = new Transaction(logManager, allocator, "CREATE_TABLE_TEST");

            var tm = mm.GetTableManager();
            int objId = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "A",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
            }, tran);

            tran.Commit();

            tran = new Transaction(logManager, allocator, "TABLE_EXISTS");

            Assert.True(tm.Exists(new TableCreateDefinition()
            {
                TableName = "A",
            }, tran));

            MetadataTable table = tm.GetById(objId, tran);
            Assert.AreEqual("A", table.TableName);

            Assert.AreEqual(new[] { "a", "b", "c" }, table.Columns.Select(t => t.ColumnName));
            Assert.AreEqual(new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }, table.Columns.Select(c => c.ColumnType));

            var cm = mm.GetColumnManager();

            tran = new Transaction(logManager, allocator, "TABLE_ITERATE");
            foreach (var c in cm.Iterate(tran))
            {
                Assert.Contains(c.ColumnName.ToString(), new[] { "a", "b", "c" });
            }
        }

        [Test]
        public void CreateMultiTable()
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = new Transaction(logManager, allocator, "SETUP");
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();
            const int repCount = 100;

            for (int i = 1; i < repCount; i++)
            {
                ITransaction tran = new Transaction(logManager, allocator, "CREATE_TABLE_TEST");
                int id = tm.CreateObject(new TableCreateDefinition()
                {
                    TableName = "T" + i,
                    ColumnNames = new[] { "a", "b", "c" },
                    ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
                }, tran);
                tran.Commit();
            }

            for (int i = 1; i < repCount; i++)
            {
                ITransaction tran = new Transaction(logManager, allocator, "GET_TABLE");
                var table = tm.GetById(i, tran);
                Assert.AreEqual("T" + i, table.TableName);

                Assert.AreEqual(new[] { "a", "b", "c" }, table.Columns.Select(c => c.ColumnName));
                Assert.AreEqual(new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }, table.Columns.Select(c => c.ColumnType));
            }
        }

        [Test]
        public void CreateWithSameName()
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = new Transaction(logManager, allocator, "SETUP");
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            var mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();
            ITransaction tran = new Transaction(logManager, allocator, "CREATE_TABLE_TEST");
            int objId = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "A",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
            }, tran);
            tran.Commit();

            Assert.Throws<ElementWithSameNameExistsException>(() =>
            {
                tran = new Transaction(logManager, allocator, "CREATE_TABLE_TEST2");
                tm.CreateObject(new TableCreateDefinition()
                {
                    TableName = "A",
                    ColumnNames = new[] { "a" },
                    ColumnTypes = new[] { ColumnType.Int }
                }, tran);
            });
        }
    }
}
