using LockManager;
using NUnit.Framework;
using PageManager;
using System.Threading.Tasks;
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
        public async Task BufferPoolCheck()
        {
            IBufferPool bp = new BufferPool();
            IPageEvictionPolicy pageEvictionPolicy = new FifoEvictionPolicy(10, 5);
            ILockManager lm = new LockManager.LockManager();

            var pageManager =  new PageManager.PageManager(DefaultSize, pageEvictionPolicy, TestGlobals.DefaultPersistedStream, bp, lm, TestGlobals.TestFileLogger);

            await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);

            Assert.AreEqual(3, bp.PagesInPool());
        }

        [Test]
        public async Task BufferPoolAfterEviction()
        {
            IBufferPool bp = new BufferPool();
            IPageEvictionPolicy pageEvictionPolicy = new FifoEvictionPolicy(10, 5);
            ILockManager lm = new LockManager.LockManager();

            var pageManager =  new PageManager.PageManager(DefaultSize, pageEvictionPolicy, TestGlobals.DefaultPersistedStream, bp, lm, TestGlobals.TestFileLogger);

            for (int i = 0; i < 11; i++)
            {
                await pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            }

            Assert.AreEqual(6, bp.PagesInPool());
        }
    }
}
