using System;
using NUnit.Framework;
using PageManager;
using Test.Common;

namespace PageManagerTests
{
    public class InMemoryPageManagerTests
    {
        private const int DefaultSize = 4096;
        private const ulong DefaultPrevPage = PageManagerConstants.NullPageId;
        private const ulong DefaultNextPage = PageManagerConstants.NullPageId;
        private DummyTran tran = new DummyTran();

        [Test]
        public void VerifyAllocatePage()
        {
            var pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            IPage page1 = pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            IPage page2 = pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            IPage page3 = pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);

            Assert.IsNotNull(page1);
            Assert.IsNotNull(page2);
            Assert.IsNotNull(page3);

            Assert.IsTrue(page1.PageId() != page2.PageId());
            Assert.IsTrue(page2.PageId() != page3.PageId());
        }

        [Test]
        public void GetPageById()
        {
            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);
            var page2 = pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);

            ulong pageId = page2.PageId();

            int[] items = new int[] { 1, 2, 3 };
            page2.Merge(items, new DummyTran());

            page2 = pageManager.GetPageInt(pageId, tran);

            Assert.AreEqual(items, page2.Fetch());
        }

        [Test]
        public void MixedTypePages()
        {
            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            var intPage = pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);
            var doublePage = pageManager.AllocatePageDouble(DefaultPrevPage, DefaultNextPage, tran);
            var strPage = pageManager.AllocatePageStr(DefaultPrevPage, DefaultNextPage, tran);

            Assert.AreEqual(PageType.IntPage, intPage.PageType());
            Assert.AreEqual(PageType.DoublePage, doublePage.PageType());
            Assert.AreEqual(PageType.StringPage, strPage.PageType());

            intPage = pageManager.GetPageInt(intPage.PageId(), tran);
            doublePage = pageManager.GetPageDouble(doublePage.PageId(), tran);
            strPage = pageManager.GetPageStr(strPage.PageId(), tran);

            Assert.AreEqual(PageType.IntPage, intPage.PageType());
            Assert.AreEqual(PageType.DoublePage, doublePage.PageType());
            Assert.AreEqual(PageType.StringPage, strPage.PageType());
        }

        [Test]
        public void GetPageOfInvalidType()
        {
            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            var intPage = pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);

            Assert.Throws<InvalidCastException>(() => { pageManager.GetPageDouble(intPage.PageId(), tran); });
        }

        [Test]
        public void PagesOfMixedType()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            MixedPage page = pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);
            page.Merge(holder, new DummyTran());

            page = pageManager.GetMixedPage(page.PageId(), tran, types);

            RowsetHolder holder2 = page.Fetch();

            Assert.AreEqual(holder2.GetIntColumn(0), intColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(1), intColumns[1]);
            Assert.AreEqual(holder2.GetDoubleColumn(2), doubleColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(3), intColumns[2]);
        }

        [Test]
        public void PageLinking()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            MixedPage page11 = pageManager.AllocateMixedPage(types, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
            MixedPage page12 = pageManager.AllocateMixedPage(types, page11.PageId(), PageManagerConstants.NullPageId, tran);
            MixedPage page13 = pageManager.AllocateMixedPage(types, page12.PageId(), PageManagerConstants.NullPageId, tran);

            Assert.AreEqual(page11.PageId(), page12.PrevPageId());
            Assert.AreEqual(page11.NextPageId(), page12.PageId());
            Assert.AreEqual(page12.NextPageId(), page13.PageId());
            Assert.AreEqual(page13.PrevPageId(), page12.PageId());
        }

        [Test]
        public void MultiPageLinking()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            MixedPage page11 = pageManager.AllocateMixedPage(types, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
            MixedPage page12 = pageManager.AllocateMixedPage(types, page11.PageId(), PageManagerConstants.NullPageId, tran);
            MixedPage page13 = pageManager.AllocateMixedPage(types, page12.PageId(), PageManagerConstants.NullPageId, tran);
            MixedPage page21 = pageManager.AllocateMixedPage(types, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
            MixedPage page22 = pageManager.AllocateMixedPage(types, page21.PageId(), PageManagerConstants.NullPageId, tran);

            Assert.AreEqual(page11.PageId(), page12.PrevPageId());
            Assert.AreEqual(page11.NextPageId(), page12.PageId());
            Assert.AreEqual(page12.NextPageId(), page13.PageId());
            Assert.AreEqual(page13.PrevPageId(), page12.PageId());

            Assert.AreEqual(page21.NextPageId(), page22.PageId());
            Assert.AreEqual(page22.PrevPageId(), page21.PageId());
        }
    }
}
