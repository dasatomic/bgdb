using DataStructures;
using LogManager;
using NUnit.Framework;
using PageManager;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Test.Common;

namespace DataStructureTests
{
    public class PageListCollectionTests
    {
        [Test]
        public async Task InitPageList()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            using ITransaction tran = new DummyTran();
            ColumnType[] types = new[] { ColumnType.Int, ColumnType.Int };

            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, new DummyTran());

            ulong cnt = 0;
            await foreach (var c in collection.Iterate(tran))
            {
                cnt += c.GetRowCount();
            }

            Assert.AreEqual(0, cnt);
        }

        [Test]
        public async Task PageListInsert()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            using ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            await collection.Add(holder, tran);

            ulong cnt = 0;
            await foreach (var c in collection.Iterate(tran))
            {
                cnt += c.GetRowCount();
            }

            Assert.AreEqual(holder.GetRowCount(), cnt);
        }

        [Test]
        public async Task PageMultiInsert()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            using ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            for (int i = 0; i < 100; i++)
            {
                await collection.Add(holder, tran);
            }

            ulong cnt = 0;
            await foreach (var c in collection.Iterate(tran))
            {
                cnt += c.GetRowCount();
            }

            Assert.AreEqual(holder.GetRowCount() * 100, cnt);
        }

        [Test]
        public async Task FilterTestNotFound()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            using ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            for (int i = 0; i < 100; i++)
            {
                await collection.Add(holder, tran);
            }

            Assert.IsEmpty(await collection.Where((holder) => holder.GetIntColumn(0).Contains(42), tran));
        }

        [Test]
        public async Task FilterTestFound()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            using ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            for (int i = 0; i < 100; i++)
            {
                await collection.Add(holder, tran);
            }

            Assert.IsNotEmpty(await collection.Where((searcher) => searcher.GetIntColumn(0).Contains(holder.GetIntColumn(0)[4]), tran));
        }

        [Test]
        public async Task IterationTests()
        {
            IAllocateMixedPage mixedPageAlloc = new PageManager.PageManager(4096, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            List<int> column0Insert = new List<int>();
            for (int i = 0; i < 1000; i++)
            {
                await collection.Add(holder, tran);
                foreach (int v in holder.GetIntColumn(0))
                {
                    column0Insert.Add(v);
                }
            }

            List<int> column0 = new List<int>();
            await foreach (var i in collection.Iterate(tran))
            {
                foreach (int v in i.GetIntColumn(0))
                {
                    column0.Add(v);
                }
            }

            Assert.AreEqual(column0Insert, column0);
        }
    }
}