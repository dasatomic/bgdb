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

            int id = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double }
            });

            var table = tm.GetById(id);

            Row[] source = new Row[] { 
                new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }),
                new Row(new[] { 2 }, new[] { 1.1 }, new[] { "notmystring" }),
                new Row(new[] { 3 }, new[] { 2.1 }, new[] { "pavle" }),
                new Row(new[] { 4 }, new[] { 1.1 }, new[] { "ivona" }),
                new Row(new[] { 1 }, new[] { 1.1 }, new[] { "zoja" }),
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
