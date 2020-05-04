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
            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            ColumnType[] types = new[] { ColumnType.Int, ColumnType.Int };

            PageListCollection collection = new PageListCollection(mixedPageAlloc, types);
            Assert.AreEqual(0, collection.Count());
        }

        [Test]
        public void PageListInsert()
        {
            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            collection.Add(holder);

            Assert.AreEqual(holder.GetRowCount(), collection.Count());
        }

        [Test]
        public void PageMultiInsert()
        {
            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            for (int i = 0; i < 100; i++)
            {
                collection.Add(holder);
            }

            Assert.AreEqual(holder.GetRowCount() * 100, collection.Count());
        }

        [Test]
        public void FilterTestNotFound()
        {

            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            for (int i = 0; i < 100; i++)
            {
                collection.Add(holder);
            }

            Assert.IsEmpty(collection.Where((holder) => holder.GetIntColumn(0).Contains(42)));
        }

        [Test]
        public void FilterTestFound()
        {

            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            for (int i = 0; i < 100; i++)
            {
                collection.Add(holder);
            }

            Assert.IsNotEmpty(collection.Where((searcher) => searcher.GetIntColumn(0).Contains(holder.GetIntColumn(0)[4])));
        }

        [Test]
        public void IterationTests()
        {
            IAllocateMixedPage mixedPageAlloc =
                new InMemoryPageManager(4096);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);
            PageListCollection collection = new PageListCollection(mixedPageAlloc, types);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            List<int> column0Insert = new List<int>();
            for (int i = 0; i < 1000; i++)
            {
                collection.Add(holder);
                foreach (int v in holder.GetIntColumn(0))
                {
                    column0Insert.Add(v);
                }
            }

            List<int> column0 = new List<int>();
            foreach (var i in collection)
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