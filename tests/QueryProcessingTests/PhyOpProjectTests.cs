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
    public class PhyOpProjectTests
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
                ClusteredIndexPositions = new int[] { }
            }, tran);

            await tran.Commit();

            tran = logManager.CreateTransaction(allocator);
            table = await tm.GetById(id, tran);
            await tran.Commit();

            List<RowHolder> source = new List<RowHolder>();
            for (int i = 0; i < 5; i++)
            {
                var rhf = new RowHolder(new[] { new ColumnInfo(ColumnType.Int), new ColumnInfo(ColumnType.String, 1), new ColumnInfo(ColumnType.Double) });
                rhf.SetField<int>(0, i);
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
            scan = new PhyOpScan(pcl, tran, table.Columns, "Table");
        }

        [Test]
        public async Task ValidateSimpleProject()
        {
            PhyOpProjectComputeFunctors functors = new PhyOpProjectComputeFunctors(
                projector: (rhf) => rhf.Project(new int[] { 0 }),
                computer: (r1, r2) => { }
            );

            PhyOpProject project = new PhyOpProject(this.scan, functors, new MetadataColumn[] { this.scan.GetOutputColumns()[0] }, null);

            List<RowHolder> result = new List<RowHolder>();
            await foreach (var row in project.Iterate(this.tran))
            {
                result.Add(row);
            }

            Assert.AreEqual(5, result.Count);
            for (int i = 0; i < 5; i++)
            {
                Assert.AreEqual(i, result[i].GetField<int>(0));
            }
        }

        [Test]
        public async Task ValidateFuncProject()
        {
            PhyOpProjectComputeFunctors functors = new PhyOpProjectComputeFunctors(
                projector: (rhf) => rhf.Project(new int[] { 0 }),
                computer: (r1, r2) =>
                {
                    int res = r1.GetField<int>(0) + r2.GetField<int>(0);
                    r2.SetField<int>(0, res);
                }
            );

            PhyOpProject project = new PhyOpProject(this.scan, functors, new MetadataColumn[] { this.scan.GetOutputColumns()[0] }, null);

            List<RowHolder> result = new List<RowHolder>();
            await foreach (var row in project.Iterate(this.tran))
            {
                result.Add(row);
            }

            Assert.AreEqual(5, result.Count);
            for (int i = 0; i < 5; i++)
            {
                Assert.AreEqual(i * 2, result[i].GetField<int>(0));
            }
        }
    }
}
