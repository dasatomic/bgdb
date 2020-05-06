using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System;
using System.Collections.Generic;
using System.Linq;

namespace QueryProcessingTests
{
    public class PhyOpScanTests
    {
        [Test]
        public void ValidateScan()
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

            Row[] source = new Row[] { 
                new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }, columnTypes),
                new Row(new[] { 2 }, new[] { 1.1 }, new[] { "notmystring" }, columnTypes),
                new Row(new[] { 3 }, new[] { 2.1 }, new[] { "pavle" }, columnTypes),
                new Row(new[] { 4 }, new[] { 1.1 }, new[] { "ivona" }, columnTypes),
                new Row(new[] { 1 }, new[] { 1.1 }, new[] { "zoja" }, columnTypes),
            };
            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic);
            op.Invoke();

            PageListCollection pcl = new PageListCollection(allocator, table.Columns.Select(x => x.ColumnType).ToArray(), allocator.GetPage(table.RootPage));
            PhyOpScan scan = new PhyOpScan(pcl, stringHeap);
            Row[] result = scan.ToArray();

            Assert.AreEqual(source, result);
        }
    }
}
