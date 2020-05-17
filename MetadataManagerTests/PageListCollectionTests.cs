using LogManager;
using MetadataManager;
using NUnit.Framework;
using PageManager;
using System.Collections.Generic;
using System.Linq;
using Test.Common;

namespace MetadataManagerTests
{
    public class PageListCollectionTests
    {
        [Test]
        public void InitPageList()
        {
            IAllocateMixedPage mixedPageAlloc = new InMemoryPageManager(4096);
            ITransaction tran = new DummyTran();
            ColumnType[] types = new[] { ColumnType.Int, ColumnType.Int };

            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, new DummyTran());
            Assert.AreEqual(0, collection.Iterate(tran).Sum(rs => rs.GetRowCount()));
        }

        [Test]
        public void PageListInsert()
        {
            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            collection.Add(holder, tran);

            Assert.AreEqual(holder.GetRowCount(), collection.Iterate(tran).Sum(rs => rs.GetRowCount()));
        }

        [Test]
        public void PageMultiInsert()
        {
            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            for (int i = 0; i < 100; i++)
            {
                collection.Add(holder, tran);
            }

            long rowCount = collection.Iterate(tran).Sum(rs => rs.GetRowCount());
            Assert.AreEqual(holder.GetRowCount() * 100, rowCount);
        }

        [Test]
        public void FilterTestNotFound()
        {

            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            for (int i = 0; i < 100; i++)
            {
                collection.Add(holder, tran);
            }

            Assert.IsEmpty(collection.Where((holder) => holder.GetIntColumn(0).Contains(42), tran));
        }

        [Test]
        public void FilterTestFound()
        {

            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            for (int i = 0; i < 100; i++)
            {
                collection.Add(holder, tran);
            }

            Assert.IsNotEmpty(collection.Where((searcher) => searcher.GetIntColumn(0).Contains(holder.GetIntColumn(0)[4]), tran));
        }

        [Test]
        public void IterationTests()
        {
            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            ITransaction tran = new DummyTran();
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            List<int> column0Insert = new List<int>();
            for (int i = 0; i < 1000; i++)
            {
                collection.Add(holder, tran);
                foreach (int v in holder.GetIntColumn(0))
                {
                    column0Insert.Add(v);
                }
            }

            List<int> column0 = new List<int>();
            foreach (var i in collection.Iterate(tran))
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