using DataStructures;
using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Test.Common;

namespace QueryProcessingTests
{
    public class PhyOpFilterTests
    {
        private PhyOpScan scan;
        private ITransaction tran;

        [SetUp]
        public async Task Setup()
        {
            ILogManager logManager;
            MetadataManager.MetadataManager metadataManager;
            IPageManager allocator;
            MetadataTable table;
            ColumnInfo[] columnInfos;

            allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = logManager.CreateTransaction(allocator);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            metadataManager = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);
            var tm = metadataManager.GetTableManager();

            tran = logManager.CreateTransaction(allocator);
            columnInfos = new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.String, 1), new ColumnInfo(ColumnType.Double) };
            int id = await tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnInfos, 
            }, tran);

            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);
            table = await tm.GetById(id, tran);
            await tran.Commit();

            List<RowHolderFixed> source = new List<RowHolderFixed>();
            for (int i = 0; i < 100; i++)
            {
                var rhf = new RowHolderFixed(new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.String, 1), new ColumnInfo(ColumnType.Double) });
                rhf.SetField<int>(0, i);
                rhf.SetField(1, i.ToString().ToCharArray());
                rhf.SetField<double>(2, i + 0.1);
                source.Add(rhf);
            }

            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            tran = logManager.CreateTransaction(allocator);
            PhyOpTableInsert op = new PhyOpTableInsert(table.Collection, opStatic, tran);
            await op.Invoke();
            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);
            PageListCollection pcl = new PageListCollection(allocator, columnInfos, table.RootPage);
            scan = new PhyOpScan(pcl, tran);
        }

        [Test]
        public async Task ValidateFilterInt()
        {
            (Func<RowHolderFixed, bool>, int)[] funcArr = new (Func<RowHolderFixed, bool>, int)[]
            {
                (((RowHolderFixed) => RowHolderFixed.GetField<int>(0) < 50), 50),
                ((RowHolderFixed) => RowHolderFixed.GetField<int>(0) >= 50, 50),
                ((RowHolderFixed) => RowHolderFixed.GetStringField(1) == "50".ToCharArray(), 1),
                ((RowHolderFixed) => RowHolderFixed.GetStringField(1) == "1".ToCharArray(), 1),
                ((RowHolderFixed) => RowHolderFixed.GetField<double>(2) >= 50, 50),
            };

            PhyOpFilter filter = new PhyOpFilter(this.scan, (rhf) => rhf.GetField<int>(0) < 50);

            List<RowHolderFixed> result = new List<RowHolderFixed>();
            await foreach (var row in filter.Iterate(this.tran))
            {
                result.Add(row);
            }

            Assert.AreEqual(50, result.Count);
        }
    }
}
