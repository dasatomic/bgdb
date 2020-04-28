using System;
using NUnit.Framework;
using PageManager;

namespace PageManagerTests
{
    public class InMemoryPageManagerTests
    {
        private const int DefaultSize = 4096;

        [Test]
        public void VerifyAllocatePage()
        {
            var pageManager = new InMemoryPageManager(DefaultSize);

            IPage page1 = pageManager.AllocatePage(PageType.IntPage);
            IPage page2 = pageManager.AllocatePage(PageType.IntPage);
            IPage page3 = pageManager.AllocatePage(PageType.IntPage);

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

            var emptyPage = pageManager.AllocatePageInt();
            var page2 = pageManager.AllocatePageInt();

            ulong pageId = page2.PageId();

            int[] items = new int[] { 1, 2, 3 };
            page2.Serialize(items);

            pageManager.SavePage(page2);
            page2 = pageManager.GetPageInt(pageId);

            Assert.AreEqual(items, page2.Deserialize());
        }
    }
}
