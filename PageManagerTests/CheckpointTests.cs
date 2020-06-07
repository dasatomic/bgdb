using NUnit.Framework;
using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Test.Common;

namespace PageManagerTests
{
    public class CheckpointTests
    {
        private const int DefaultSize = 4096;
        private const ulong DefaultPrevPage = PageManagerConstants.NullPageId;
        private const ulong DefaultNextPage = PageManagerConstants.NullPageId;
        private DummyTran tran = new DummyTran();

        [Test]
        public async Task CheckpointFlush()
        {
            PersistedStream persistedStream = new PersistedStream(1024 * 1024, "checkpoint.data", createNew: true);
            IBufferPool bp = new BufferPool();
            using var pageManager =  new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, persistedStream, bp);

            pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            pageManager.AllocatePage(PageType.IntPage, DefaultPrevPage, DefaultNextPage, tran);
            var p = pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);

            Assert.IsTrue(!bp.GetAllDirtyPages().Any());
            p.Merge(new int[] { 1, 2, 3 }, tran);
            Assert.IsTrue(bp.GetAllDirtyPages().Any());

            await pageManager.Checkpoint();

            Assert.IsTrue(!bp.GetAllDirtyPages().Any());
        }

        [Test]
        public async Task AttachAfterCheckpoint()
        {
            PersistedStream persistedStream = new PersistedStream(1024 * 1024, "checkpoint.data", createNew: true);
            IBufferPool bp = new BufferPool();
            IntegerOnlyPage p1, p2, p3;
            using (var pageManager = new PageManager.PageManager(DefaultSize, TestGlobals.DefaultEviction, persistedStream, bp))
            {
                p1 = pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);
                p2 = pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);
                p3 = pageManager.AllocatePageInt(DefaultPrevPage, DefaultNextPage, tran);

                p1.Merge(new int[] { 1, 2, 3 }, tran);
                p2.Merge(new int[] { 3, 2, 1 }, tran);
                p3.Merge(new int[] { 1, 2, 4 }, tran);

                await pageManager.Checkpoint();
            }

            PersistedStream persistedStream2 = new PersistedStream(1024 * 1024, "checkpoint.data", createNew: false);
            var eviction = new FifoEvictionPolicy(10, 5);
            using var pageManager2 =  new PageManager.PageManager(DefaultSize, eviction, persistedStream2);

            var readPage = pageManager2.GetPageInt(p1.PageId(), tran);
            Assert.AreEqual(p1, readPage);
            readPage = pageManager2.GetPageInt(p2.PageId(), tran);
            Assert.AreEqual(p2, readPage);
            readPage = pageManager2.GetPageInt(p3.PageId(), tran);
            Assert.AreEqual(p3, readPage);
        }
    }
}
