using NUnit.Framework;
using MetadataManager;
using PageManager;
using System.Collections.Generic;
using System.Linq;

namespace MetadataManager
{
    public class TableManagerTests
    {
        [Test]
        public void CreateTable()
        {
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            MetadataManager mm = new MetadataManager(allocator, stringHeap, allocator);

            var tm = mm.GetTableManager();
            int objId = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "A",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
            });

            Assert.True(tm.Exists(new TableCreateDefinition()
            {
                TableName = "A",
            }));

            MetadataTable table = tm.GetById(objId);
            Assert.AreEqual("A", table.TableName);

            Assert.AreEqual(new[] { "a", "b", "c" }, table.Columns.Select(t => t.ColumnName));
            Assert.AreEqual(new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }, table.Columns.Select(c => c.ColumnType));

            var cm = mm.GetColumnManager();

            foreach (var c in cm)
            {
                Assert.Contains(c.ColumnName.ToString(), new[] { "a", "b", "c" });
            }
        }

        [Test]
        public void CreateMultiTable()
        {
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            MetadataManager mm = new MetadataManager(allocator, stringHeap, allocator);

            var tm = mm.GetTableManager();
            const int repCount = 100;

            for (int i = 1; i < repCount; i++)
            {
                int id = tm.CreateObject(new TableCreateDefinition()
                {
                    TableName = "T" + i,
                    ColumnNames = new[] { "a", "b", "c" },
                    ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
                });
            }

            for (int i = 1; i < repCount; i++)
            {
                var table = tm.GetById(i);
                Assert.AreEqual("T" + i, table.TableName);

                Assert.AreEqual(new[] { "a", "b", "c" }, table.Columns.Select(c => c.ColumnName));
                Assert.AreEqual(new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }, table.Columns.Select(c => c.ColumnType));
            }
        }

        [Test]
        public void CreateWithSameName()
        {
            var allocator =
                new InMemoryPageManager(4096);

            StringHeapCollection stringHeap = new StringHeapCollection(allocator);
            MetadataManager mm = new MetadataManager(allocator, stringHeap, allocator);

            var tm = mm.GetTableManager();
            int objId = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "A",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
            });

            Assert.Throws<ElementWithSameNameExistsException>(() =>
            {
                tm.CreateObject(new TableCreateDefinition()
                {
                    TableName = "A",
                    ColumnNames = new[] { "a" },
                    ColumnTypes = new[] { ColumnType.Int }
                });
            });
        }
    }
}
