﻿using System;
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
        public async Task VerifyAllocatePage()
        {
            var pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            IPage page1 = await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            IPage page2 = await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            IPage page3 = await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);

            Assert.IsNotNull(page1);
            Assert.IsNotNull(page2);
            Assert.IsNotNull(page3);

            Assert.IsTrue(page1.PageId() != page2.PageId());
            Assert.IsTrue(page2.PageId() != page3.PageId());
        }

        [Test]
        public async Task GetPageById()
        {
            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            await pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);
            var page2 = await pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);

            ulong pageId = page2.PageId();

            int[] items = new int[] { 1, 2, 3 };
            page2.Merge(items, new DummyTran());

            page2 = await pageManager.GetPageInt(pageId, tran);

            Assert.AreEqual(items, page2.Fetch(TestGlobals.DummyTran));
        }

        [Test]
        public async Task MixedTypePages()
        {
            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);

            var intPage = await pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);
            var doublePage = await pageManager.AllocatePageDouble(DefaultPrevPage, DefaultNextPage, tran);
            var strPage = await pageManager.AllocatePageStr(DefaultPrevPage, DefaultNextPage, tran);

            Assert.AreEqual(PageType.IntPage, intPage.PageType());
            Assert.AreEqual(PageType.DoublePage, doublePage.PageType());
            Assert.AreEqual(PageType.StringPage, strPage.PageType());

            intPage = await pageManager.GetPageInt(intPage.PageId(), tran);
            doublePage = await pageManager.GetPageDouble(doublePage.PageId(), tran);
            strPage = await pageManager.GetPageStr(strPage.PageId(), tran);

            Assert.AreEqual(PageType.IntPage, intPage.PageType());
            Assert.AreEqual(PageType.DoublePage, doublePage.PageType());
            Assert.AreEqual(PageType.StringPage, strPage.PageType());
        }

        [Test]
        public async Task GetPageOfInvalidType()
        {
            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            var intPage = await pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);

            Assert.ThrowsAsync<InvalidCastException>(async () => { await pageManager.GetPageDouble(intPage.PageId(), tran); });
        }

        [Test]
        public async Task PagesOfMixedType()
        {
            GenerateDataUtils.GenerateSampleData(out ColumnType[] types, out int[][] intColumns, out double[][] doubleColumns, out long[][] pagePointerColumns, out PagePointerOffsetPair[][] pagePointerOffsetColumns);

            IPageManager pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            MixedPage page = await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);

            RowsetHolder holder = new RowsetHolder(types);
            holder.SetColumns(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);
            page.Merge(holder, new DummyTran());

            page = await pageManager.GetMixedPage(page.PageId(), tran, types);

            RowsetHolder holder2 = page.Fetch(TestGlobals.DummyTran);

            Assert.AreEqual(holder2.GetIntColumn(0), intColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(1), intColumns[1]);
            Assert.AreEqual(holder2.GetDoubleColumn(2), doubleColumns[0]);
            Assert.AreEqual(holder2.GetIntColumn(3), intColumns[2]);
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

            IPage page1 = await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            IPage page2 = await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            IPage page3 = await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);

            var allocationMaps = pageManager.GetAllocationMapFirstPage();
            Assert.IsTrue(allocationMaps.Count == 1);
            int[] items = allocationMaps.First().Fetch(TestGlobals.DummyTran);
            Assert.AreEqual(1, items.Length);

            int expectedMask = 1 << (int)page1.PageId() | 1 << (int)page2.PageId() | 1 << (int)page3.PageId();
            Assert.AreEqual(expectedMask, items[0]);
        }

        [Test]
        public async Task VerifyAllocationMapMultiple()
        {
            var pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, TestGlobals.DefaultPersistedStream);
            List<ulong> pageIds = new List<ulong>();

            for (int i = 2; i < 32 * 10 + 5; i++)
            {
                IPage page = await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
                pageIds.Add(page.PageId());
            }

            var allocationMaps = pageManager.GetAllocationMapFirstPage();
            Assert.AreEqual(1, allocationMaps.Count);

            int[] items = allocationMaps.First().Fetch(TestGlobals.DummyTran);
            Assert.AreEqual(11, items.Length);
            foreach (ulong pageId in pageIds)
            {
                Assert.IsTrue((items[pageId / 32] & (1 << ((int)pageId % 32))) != 0);
            }
        }
    }
}