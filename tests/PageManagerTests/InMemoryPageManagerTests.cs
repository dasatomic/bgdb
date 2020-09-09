using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
        public async Task GetPageById()
        {
            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            var strPage = await pageManager.AllocatePageStr(DefaultPrevPage, DefaultNextPage, tran);

            Assert.AreEqual(PageType.StringPage, strPage.PageType());

            strPage = await pageManager.GetPageStr(strPage.PageId(), tran);

            Assert.AreEqual(PageType.StringPage, strPage.PageType());
        }

        [Test]
        public async Task GetPageOfInvalidType()
        {
            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            MixedPage page = await pageManager.AllocateMixedPage(types, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);

            Assert.ThrowsAsync<InvalidCastException>(async () => { await pageManager.GetPageStr(page.PageId(), tran); });
        }

        [Test]
        public async Task PagesOfMixedType()
        {
            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnType[] types);

            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            MixedPage page = await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);


            rows.ForEach(r => page.Insert(r, tran));
            page = await pageManager.GetMixedPage(page.PageId(), tran, types);
            Assert.AreEqual(rows.ToArray(), page.Fetch(TestGlobals.DummyTran).ToArray());
        }

        [Test]
        public async Task PageLinking()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            MixedPage page11 = await pageManager.AllocateMixedPage(types, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
            MixedPage page12 = await pageManager.AllocateMixedPage(types, page11.PageId(), PageManagerConstants.NullPageId, tran);
            MixedPage page13 = await pageManager.AllocateMixedPage(types, page12.PageId(), PageManagerConstants.NullPageId, tran);

            Assert.AreEqual(page11.PageId(), page12.PrevPageId());
            Assert.AreEqual(page11.NextPageId(), page12.PageId());
            Assert.AreEqual(page12.NextPageId(), page13.PageId());
            Assert.AreEqual(page13.PrevPageId(), page12.PageId());
        }

        [Test]
        public async Task MultiPageLinking()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            MixedPage page11 = await pageManager.AllocateMixedPage(types, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
            MixedPage page12 = await pageManager.AllocateMixedPage(types, page11.PageId(), PageManagerConstants.NullPageId, tran);
            MixedPage page13 = await pageManager.AllocateMixedPage(types, page12.PageId(), PageManagerConstants.NullPageId, tran);
            MixedPage page21 = await pageManager.AllocateMixedPage(types, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
            MixedPage page22 = await pageManager.AllocateMixedPage(types, page21.PageId(), PageManagerConstants.NullPageId, tran);

            Assert.AreEqual(page11.PageId(), page12.PrevPageId());
            Assert.AreEqual(page11.NextPageId(), page12.PageId());
            Assert.AreEqual(page12.NextPageId(), page13.PageId());
            Assert.AreEqual(page13.PrevPageId(), page12.PageId());

            Assert.AreEqual(page21.NextPageId(), page22.PageId());
            Assert.AreEqual(page22.PrevPageId(), page21.PageId());
        }

        [Test]
        public async Task VerifyAllocationMap()
        {
            var pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            IPage page1 = await pageManager.AllocatePage(PageType.StringPage, DefaultPrevPage, DefaultNextPage, tran);
            IPage page2 = await pageManager.AllocatePage(PageType.StringPage, DefaultPrevPage, DefaultNextPage, tran);
            IPage page3 = await pageManager.AllocatePage(PageType.StringPage, DefaultPrevPage, DefaultNextPage, tran);

            var allocationMaps = pageManager.GetAllocationMapFirstPage();
            Assert.IsTrue(allocationMaps.Count == 1);
            int[] items = allocationMaps.First().FindAllSet(new DummyTran()).ToArray();
            Assert.AreEqual(new int[] { (int)page1.PageId(), (int)page2.PageId(), (int)page3.PageId() }, items);
        }

        [Test]
        public async Task VerifyAllocationMapMultiple()
        {
            var pageManager =  new PageManager.PageManager(DefaultSize, new FifoEvictionPolicy(1000, 1), TestGlobals.DefaultPersistedStream);
            List<ulong> pageIds = new List<ulong>();

            for (int i = 2; i < 32 * 10 + 5; i++)
            {
                IPage page = await pageManager.AllocatePage(PageType.StringPage, DefaultPrevPage, DefaultNextPage, tran);
                pageIds.Add(page.PageId());
            }

            var allocationMaps = pageManager.GetAllocationMapFirstPage();
            Assert.AreEqual(1, allocationMaps.Count);

            int[] items = allocationMaps.First().FindAllSet(new DummyTran()).ToArray();
            Assert.AreEqual(pageIds.ToArray(), items);
        }
    }
}
