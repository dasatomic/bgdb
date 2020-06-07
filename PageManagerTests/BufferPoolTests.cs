using NUnit.Framework;
using PageManager;
using Test.Common;

namespace PageManagerTests
{
    public class BufferPoolTests
    {
        private const int DefaultSize = 4096;
        private const ulong DefaultPrevPage = PageManagerConstants.NullPageId;
        private const ulong DefaultNextPage = PageManagerConstants.NullPageId;
        private DummyTran tran = new DummyTran();

        [Test]
        public void BufferPoolCheck()
        {
            IBufferPool bp = new BufferPool();
            IPageEvictionPolicy pageEvictionPolicy = new FifoEvictionPolicy(10, 5);

            var pageManager =  new PageManager.PageManager(DefaultSize, pageEvictionPolicy, TestGlobals.DefaultPersistedStream, bp);

            pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);

            Assert.AreEqual(3, bp.PagesInPool());
        }

        [Test]
        public void BufferPoolAfterEviction()
        {
            IBufferPool bp = new BufferPool();
            IPageEvictionPolicy pageEvictionPolicy = new FifoEvictionPolicy(10, 5);

            var pageManager =  new PageManager.PageManager(DefaultSize, pageEvictionPolicy, TestGlobals.DefaultPersistedStream, bp);

            for (int i = 0; i < 11; i++)
            {
                pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            }

            Assert.AreEqual(6, bp.PagesInPool());
        }
    }
}
