using DataStructures;
using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Test.Common;

namespace QueryProcessingTests
{
    public class PhyOpScanTests
    {
        [Test]
        public async Task ValidateScan()
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = logManager.CreateTransaction(allocator);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();

            ITransaction tran = logManager.CreateTransaction(allocator);
            var columnTypes = new[] { ColumnType.Int, ColumnType.StringPointer, ColumnType.Double };
            int id = await tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnTypes, 
            }, tran);

            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);
            var table = await tm.GetById(id, tran);
            await tran.Commit();

            Row[] source = new Row[] { 
                new Row(new[] { 1 }, new[] { 1.1 }, new[] { "mystring" }, columnTypes),
                new Row(new[] { 2 }, new[] { 1.1 }, new[] { "notmystring" }, columnTypes),
                new Row(new[] { 3 }, new[] { 2.1 }, new[] { "pavle" }, columnTypes),
                new Row(new[] { 4 }, new[] { 1.1 }, new[] { "ivona" }, columnTypes),
                new Row(new[] { 1 }, new[] { 1.1 }, new[] { "zoja" }, columnTypes),
            };
            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);


            tran = logManager.CreateTransaction(allocator);
            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic, tran);
            await op.Invoke();
            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);
            PageListCollection pcl = new PageListCollection(allocator, columnTypes, table.RootPage);
            PhyOpScan scan = new PhyOpScan(pcl, stringHeap, tran);

            List<Row> result = new List<Row>();

            await foreach (var row in scan.Iterate(tran))
            {
                result.Add(row);
            }

            Assert.AreEqual(source, result.ToArray());
        }
    }
}
