using NUnit.Framework;
using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Test.Common;

namespace PageManagerTests
{
    class MixedModePageTests
    {
        private const int DefaultSize = 4096;
        private const int DefaultPageId = 42;
        private const int DefaultPrevPage = 41;
        private const int DefaultNextPage = 43;

        [Test]
        public void Merge()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types1, out int[][] intColumns1, out double[][] doubleColumns1, out long[][] pagePointerColumns1, out PagePointerOffsetPair[][] pagePointerOffsetColumns1);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] _, out int[][] intColumns2, out double[][] doubleColumns2, out long[][] pagePointerColumns2, out PagePointerOffsetPair[][] pagePointerOffsetColumns2);
            MixedPage page = new MixedPage(DefaultSize, DefaultPageId, types1, DefaultPrevPage, DefaultNextPage);

            RowsetHolder holder1 = new RowsetHolder(types1);
            holder1.SetColumns(intColumns1, doubleColumns1, pagePointerOffsetColumns1, pagePointerColumns1);
            RowsetHolder holder2 = new RowsetHolder(types1);
            holder2.SetColumns(intColumns2, doubleColumns2, pagePointerOffsetColumns2, pagePointerColumns2);

            page.Store(holder1);
            page.Merge(holder2);

            RowsetHolder result = page.Fetch();

            Assert.AreEqual(result.GetIntColumn(0), intColumns1[0].Concat(intColumns2[0]).ToArray());
            Assert.AreEqual(result.GetIntColumn(1), intColumns1[1].Concat(intColumns2[1]).ToArray());
            Assert.AreEqual(result.GetDoubleColumn(2), doubleColumns1[0].Concat(doubleColumns2[0]).ToArray());
            Assert.AreEqual(result.GetIntColumn(3), intColumns1[2].Concat(intColumns2[2]).ToArray());
        }
    }
}
