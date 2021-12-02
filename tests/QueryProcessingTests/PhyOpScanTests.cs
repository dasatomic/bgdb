using DataStructures;
using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Test.Common;

namespace QueryProcessingTests
{
    public class PhyOpScanTests
    {
        public enum IndexState
        {
            NoIndex,
            IndexCol0,
            IndexCol2,
        }

        [Test]
        [TestCase(IndexState.NoIndex)]
        [TestCase(IndexState.IndexCol0)]
        [TestCase(IndexState.IndexCol2)]
        public async Task ValidateScan(IndexState indexState)
        {
            var allocator =  new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            ILogManager logManager = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            ITransaction setupTran = logManager.CreateTransaction(allocator);
            StringHeapCollection stringHeap = new StringHeapCollection(allocator, setupTran);
            MetadataManager.MetadataManager mm = new MetadataManager.MetadataManager(allocator, stringHeap, allocator, logManager);

            var tm = mm.GetTableManager();
            int[] clusteredIndexPosition = null;

            switch (indexState)
            {
                case IndexState.NoIndex:
                    clusteredIndexPosition = new int[0];
                    break;
                case IndexState.IndexCol0:
                    clusteredIndexPosition = new int[] { 0 };
                    break;
                case IndexState.IndexCol2:
                    clusteredIndexPosition = new int[] { 2 };
                    break;
                default:
                    Assert.Fail();
                    break;
            }

            ITransaction tran = logManager.CreateTransaction(allocator);
            var columnInfos = new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.String, 1), new ColumnInfo(ColumnType.Double) };
            int id = await tm.CreateObject(new TableCreateDefinition()
            {
                TableName = "Table",
                ColumnNames = new[] { "a", "b", "c" },
                ColumnTypes = columnInfos, 
                ClusteredIndexPositions = clusteredIndexPosition,
            }, tran);

            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);
            var table = await tm.GetById(id, tran);
            await tran.Commit();

            List<RowHolder> source = new List<RowHolder>();
            for (int i = 0; i < 5; i++)
            {
                var rhf = new RowHolder(new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.String, 1), new ColumnInfo(ColumnType.Double) });
                rhf.SetField<int>(0, i);
                rhf.SetField(1, i.ToString().ToCharArray());
                rhf.SetField<double>(2, i + 1.1);
                source.Add(rhf);
            }

            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            tran = logManager.CreateTransaction(allocator);
            PhyOpTableInsert op = new PhyOpTableInsert(table.Collection, opStatic);
            await op.Iterate(tran).AllResultsAsync();
            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);
            PageListCollection pcl = new PageListCollection(allocator, columnInfos, table.RootPage);
            PhyOpScan scan = new PhyOpScan(pcl, tran, table.Columns, "Table");

            List<RowHolder> result = new List<RowHolder>();

            await foreach (var row in scan.Iterate(tran))
            {
                result.Add(row);
            }

            Assert.AreEqual(source, result.ToArray());
        }
    }
}
