using System;
using NUnit.Framework;
using PageManager;

namespace PageManagerTests
{
    public class InMemoryPageManagerTests
    {
        private const int DefaultSize = 4096;
        private const ulong DefaultPrevPage = 0;
        private const ulong DefaultNextPage = 0;

        [Test]
        public void VerifyAllocatePage()
        {
            var pageManager = new InMemoryPageManager(DefaultSize);

            IPage page1 = pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage);
            IPage page2 = pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage);
            IPage page3 = pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage);

            Assert.IsNotNull(page1);
            Assert.IsNotNull(page2);
            Assert.IsNotNull(page3);

            Assert.IsTrue(page1.PageId() != page2.PageId());
            Assert.IsTrue(page2.PageId() != page3.PageId());
        }

        [Test]
        public void GetPageById()
        {
            var pageManager = new InMemoryPageManager(DefaultSize);

            pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage);
            var page2 = pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage);

            ulong pageId = page2.PageId();

            int[] items = new int[] { 1, 2, 3 };
            page2.Serialize(items);

            pageManager.SavePage(page2);
            page2 = pageManager.GetPageInt(pageId);

            Assert.AreEqual(items, page2.Deserialize());
        }

        [Test]
        public void MixedTypePages()
        {
            var pageManager = new InMemoryPageManager(DefaultSize);

            var intPage = pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage);
            var doublePage = pageManager.AllocatePageDouble(DefaultPrevPage, DefaultNextPage);
            var strPage = pageManager.AllocatePageStr(DefaultPrevPage, DefaultNextPage);

            Assert.AreEqual(PageType.IntPage, intPage.PageType());
            Assert.AreEqual(PageType.DoublePage, doublePage.PageType());
            Assert.AreEqual(PageType.StringPage, strPage.PageType());

            pageManager.SavePage(intPage);
            pageManager.SavePage(doublePage);
            pageManager.SavePage(strPage);

            intPage = pageManager.GetPageInt(intPage.PageId());
            doublePage = pageManager.GetPageDouble(doublePage.PageId());
            strPage = pageManager.GetPageStr(strPage.PageId());

            Assert.AreEqual(PageType.IntPage, intPage.PageType());
            Assert.AreEqual(PageType.DoublePage, doublePage.PageType());
            Assert.AreEqual(PageType.StringPage, strPage.PageType());
        }

        [Test]
        public void GetPageOfInvalidType()
        {
            var pageManager = new InMemoryPageManager(DefaultSize);
            var intPage = pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage);
            pageManager.SavePage(intPage);

            Assert.Throws<InvalidCastException>(() => { pageManager.GetPageDouble(intPage.PageId()); });
        }

        [Test]
        public void PagesOfMixedType()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            var pageManager = new InMemoryPageManager(DefaultSize);
            MixedPage page = pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);
            page.Serialize(holder);

            pageManager.SavePage(page);
            page = pageManager.GetMixedPage(page.PageId());

            RowsetHolder holder2 = page.Deserialize();

            Assert.AreEqual(holder2.GetIntColumn(0), intColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(1), intColumns[1]);
            Assert.AreEqual(holder2.GetDoubleColumn(2), doubleColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(3), intColumns[2]);
        }

        [Test]
        public void PageLinking()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            var pageManager = new InMemoryPageManager(DefaultSize);
            MixedPage page11 = pageManager.AllocateMixedPage(types, 0, 0);
            MixedPage page12 = pageManager.AllocateMixedPage(types, page11.PageId(), 0);
            MixedPage page13 = pageManager.AllocateMixedPage(types, page12.PageId(), 0);

            Assert.AreEqual(page11.PageId(), page12.PrevPageId());
            Assert.AreEqual(page11.NextPageId(), page12.PageId());
            Assert.AreEqual(page12.NextPageId(), page13.PageId());
            Assert.AreEqual(page13.PrevPageId(), page12.PageId());
        }

        [Test]
        public void MultiPageLinking()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            var pageManager = new InMemoryPageManager(DefaultSize);
            MixedPage page11 = pageManager.AllocateMixedPage(types, 0, 0);
            MixedPage page12 = pageManager.AllocateMixedPage(types, page11.PageId(), 0);
            MixedPage page13 = pageManager.AllocateMixedPage(types, page12.PageId(), 0);
            MixedPage page21 = pageManager.AllocateMixedPage(types, 0, 0);
            MixedPage page22 = pageManager.AllocateMixedPage(types, page21.PageId(), 0);

            Assert.AreEqual(page11.PageId(), page12.PrevPageId());
            Assert.AreEqual(page11.NextPageId(), page12.PageId());
            Assert.AreEqual(page12.NextPageId(), page13.PageId());
            Assert.AreEqual(page13.PrevPageId(), page12.PageId());

            Assert.AreEqual(page21.NextPageId(), page22.PageId());
            Assert.AreEqual(page22.PrevPageId(), page21.PageId());
        }
    }
}
