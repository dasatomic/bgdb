using NUnit.Framework;
using MetadataManager;
using PageManager;
using System.Collections.Generic;

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

            Assert.AreEqual("A", tm.GetById(objId).TableName);

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

            for (int i = 1; i < 1000; i++)
            {
                int id = tm.CreateObject(new TableCreateDefinition()
                {
                    TableName = "T" + i,
                    ColumnNames = new[] { "a", "b", "c" },
                    ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
                });
            }

            for (int i = 1; i < 1000; i++)
            {
                var table = tm.GetById(i);
                Assert.AreEqual("T" + i, table.TableName);
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
