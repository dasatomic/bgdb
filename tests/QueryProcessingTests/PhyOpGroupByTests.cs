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
    public class PhyOpGroupByTests
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

            List<RowHolder> source = new List<RowHolder>();
            for (int i = 0; i < 100; i++)
            {
                var rhf = new RowHolder(new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.String, 1), new ColumnInfo(ColumnType.Double) });
                rhf.SetField<int>(0, i % 3);
                rhf.SetField(1, i.ToString().ToCharArray());
                rhf.SetField<double>(2, i + 0.1);
                source.Add(rhf);
            }

            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            tran = logManager.CreateTransaction(allocator);
            PhyOpTableInsert op = new PhyOpTableInsert(table.Collection, opStatic);
            await op.Iterate(tran).AllResultsAsync();
            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);
            PageListCollection pcl = new PageListCollection(allocator, columnInfos, table.RootPage);
            scan = new PhyOpScan(pcl, tran);
        }

        [Test]
        public async Task ValidateGroupBy()
        {
            GroupByFunctors functors = new GroupByFunctors(
                projector: (rhf) => rhf.Project(new int[] { 0, 1 }),
                grouper: (rhf) => rhf.Project(new int[] { 0 }),
                aggs: (rhf, state) =>
                    {
                        if (new string(rhf.GetStringField(1)).CompareTo(new string(state.GetStringField(1))) == 1)
                        {
                            state.SetField(1, rhf.GetStringField(1));
                        }

                        return state;
                    },
                projectColumnInfo: null
                );

            PhyOpGroupBy groupBy = new PhyOpGroupBy(this.scan, functors);

            List<RowHolder> result = new List<RowHolder>();
            await foreach (var row in groupBy.Iterate(this.tran))
            {
                result.Add(row);
            }

            Assert.AreEqual(3, result.Count);

            Assert.AreEqual(0, result[0].GetField<int>(0));
            Assert.AreEqual("99".ToCharArray(), result[0].GetStringField(1));

            Assert.AreEqual(1, result[1].GetField<int>(0));
            Assert.AreEqual("97".ToCharArray(), result[1].GetStringField(1));

            Assert.AreEqual(2, result[2].GetField<int>(0));
            Assert.AreEqual("98".ToCharArray(), result[2].GetStringField(1));
        }

        [Test]
        public async Task ValidateGroupByColumnMappingChange()
        {
            GroupByFunctors functors = new GroupByFunctors(
                projector: (rhf) => rhf.Project(new int[] { 0, 2 }),
                grouper: (rhf) => rhf.Project(new int[] { 0 }),
                aggs: (rhf, state) =>
                    {
                        if (rhf.GetField<double>(1) > state.GetField<double>(1))
                        {
                            state.SetField(1, rhf.GetField<double>(1));
                        }

                        return state;
                    },
                projectColumnInfo: null
                );

            PhyOpGroupBy groupBy = new PhyOpGroupBy(this.scan, functors);

            List<RowHolder> result = new List<RowHolder>();
            await foreach (var row in groupBy.Iterate(this.tran))
            {
                result.Add(row);
            }

            Assert.AreEqual(3, result.Count);

            Assert.AreEqual(0, result[0].GetField<int>(0));
            Assert.AreEqual(99.1, result[0].GetField<double>(1));

            Assert.AreEqual(1, result[1].GetField<int>(0));
            Assert.AreEqual(97.1, result[1].GetField<double>(1));

            Assert.AreEqual(2, result[2].GetField<int>(0));
            Assert.AreEqual(98.1, result[2].GetField<double>(1));
        }
    }
}
