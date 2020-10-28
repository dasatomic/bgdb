using DataStructures;
using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using QueryProcessing;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Test.Common;

namespace QueryProcessingTests
{
    public class PhyOpOrderByTests
    {
        private const int Asc = 0;
        private const int Desc = 1;
        private PhyOpScan scan;
        private ITransaction tran;

        private delegate int CompareDelegate(RowHolder left, RowHolder right, int columnId);

        [SetUp]
        public async Task Setup()
        {
            ILogManager logManager;
            MetadataManager.MetadataManager metadataManager;
            IPageManager allocator;
            MetadataTable table;
            ColumnInfo[] columnInfos;

            allocator = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
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
                rhf.SetField<int>(0, i % 10);
                rhf.SetField(1, i.ToString().ToCharArray());
                rhf.SetField<double>(2, 100 - i + 0.1);
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

        [Test, Pairwise]
        public async Task ValidateOrderByOneRow(
            [Values(Asc, Desc)] int column1Dir,
            [Range(0, 2)] int column1Id)
        {
            await ValidateOrderBy(scan, tran, new int[] { column1Id }, new int[] { column1Dir });
        }

        [Test, Pairwise]
        public async Task ValidateOrderByTwoRows(
            [Values(Asc, Desc)] int column1Dir,
            [Values(Asc, Desc)] int column2Dir,
            [Range(0, 2)] int column1Id,
            [Range(0, 2)] int column2Id)
        {
            await ValidateOrderBy(scan, tran, new int[] { column1Id, column2Id }, new int[] { column1Dir, column2Dir });
        }

        [Test, Pairwise]
        public async Task ValidateOrderByThreeRows(
            [Values(Asc, Desc)] int column1Dir,
            [Values(Asc, Desc)] int column2Dir,
            [Values(Asc, Desc)] int column3Dir,
            [Range(0, 2)] int column1Id,
            [Range(0, 2)] int column2Id,
            [Range(0, 2)] int column3Id)
        {
            await ValidateOrderBy(scan, tran, new int[] { column1Id, column2Id, column3Id }, new int[] { column1Dir, column2Dir, column3Dir });
        }

        #region Helper

        private async Task ValidateOrderBy(IPhysicalOperator<RowHolder> source, ITransaction transaction, int[] columnIds, int[] directions)
        {
            PhyOpOrderBy orderBy = CreatePhyOpOrderBy(source, columnIds, directions);
            await ValidateResultIsOrdered(orderBy, transaction, columnIds, directions);
        }

        private PhyOpOrderBy CreatePhyOpOrderBy(IPhysicalOperator<RowHolder> source, int[] columnIds, int[] directions)
        {
            return new PhyOpOrderBy(source, new RowHolderOrderByComparer(GetOrderByColumns(source.GetOutputColumns(), columnIds, directions)));
        }

        private OrderByColumn[] GetOrderByColumns(MetadataColumn[] columns, int[] columnIds, int[] directions)
        {
            OrderByColumn[] orderByColumns = new OrderByColumn[columnIds.Length];

            for (int i = 0; i < columnIds.Length; i++)
            {
                orderByColumns[i] = new OrderByColumn(columns[columnIds[i]], GetDirection(directions[i]));
            }

            return orderByColumns;
        }

        private OrderByColumn.Direction GetDirection(int direction)
        {
            if (direction == Asc) return OrderByColumn.Direction.Asc;
            if (direction == Desc) return OrderByColumn.Direction.Desc;
            throw new ArgumentException();
        }

        private async Task ValidateResultIsOrdered(IPhysicalOperator<RowHolder> source, ITransaction transaction, int[] columnIds, int[] directions)
        {
            RowHolder? previous = null;
            MetadataColumn[] columns = source.GetOutputColumns();

            await foreach (RowHolder current in source.Iterate(transaction))
            {
                if (previous != null)
                {
                    for (int i = 0; i < columnIds.Length; ++i)
                    {
                        int result = Compare(columns[columnIds[i]], previous.Value, current, columnIds[i]);
                        if (directions[i] == Asc) Assert.IsTrue(result <= 0);
                        else Assert.IsTrue(result >= 0);

                        if (result != 0) break;
                    }
                }
                previous = current;
            }
        }

        private int Compare(MetadataColumn mc, RowHolder left, RowHolder right, int columnId)
        {
            if (mc.ColumnType.ColumnType == ColumnType.Int)
            {
                int l = left.GetField<int>(columnId);
                int r = right.GetField<int>(columnId);
                return l.CompareTo(r);
            }
            else if (mc.ColumnType.ColumnType == ColumnType.String)
            {
                string l = new string(left.GetStringField(columnId));
                string r = new string(right.GetStringField(columnId));
                return l.CompareTo(r);
            }
            else if (mc.ColumnType.ColumnType == ColumnType.Double)
            {
                double l = left.GetField<double>(columnId);
                double r = right.GetField<double>(columnId);
                return l.CompareTo(r);
            }
            else
            {
                throw new ArgumentException();
            }
        }
    }
    #endregion Helper
}
