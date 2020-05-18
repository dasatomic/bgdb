using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace QueryProcessingTests
{
    public class PhyOpScanTests
    {
        [Test]
        public void ValidateScan()
        {
            var allocator = new InMemoryPageManager(4096);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = new Transaction(logManager, allocator, "SETUP");
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();

            ITransaction tran = new Transaction(logManager, allocator, "CREATE_TABLE_TEST");
            var columnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double };
            int id = tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnTypes, 
            }, tran);

            tran.Commit();

            tran = new Transaction(logManager, allocator, "GET_TABLE");
            var table = tm.GetById(id, tran);
            tran.Commit();

            Row[] source = new Row[] { 
                new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }, columnTypes),
                new Row(new[] { 2 }, new[] { 1.1 }, new[] { "notmystring" }, columnTypes),
                new Row(new[] { 3 }, new[] { 2.1 }, new[] { "pavle" }, columnTypes),
                new Row(new[] { 4 }, new[] { 1.1 }, new[] { "ivona" }, columnTypes),
                new Row(new[] { 1 }, new[] { 1.1 }, new[] { "zoja" }, columnTypes),
            };
            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);


            tran = new Transaction(logManager, allocator, "INSERT");
            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic, tran);
            op.Invoke();
            tran.Commit();

            tran = new Transaction(logManager, allocator, "SELECT");
            PageListCollection pcl = new PageListCollection(allocator, table.Columns.Select(x => x.ColumnType).ToArray(), allocator.GetPage(table.RootPage, tran));
            PhyOpScan scan = new PhyOpScan(pcl, stringHeap, tran);
            Row[] result = scan.Iterate(tran).ToArray();

            Assert.AreEqual(source, result);
        }
    }
}
