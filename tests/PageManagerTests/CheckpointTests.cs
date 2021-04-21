using LockManager;
using NUnit.Framework;
using PageManager;
using System.Linq;
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
        [MaxTime(10000)]
        public async Task CheckpointFlush()
        {
            PersistedStream persistedStream = new PersistedStream(1024 * 1024, "checkpoint.data", createNew: true);
            IBufferPool bp = new BufferPool(TestGlobals.DefaultEviction, TestGlobals.DefaultPageSize);
            ILockManager lm = new LockManager.LockManager();
            using var pageManager =  new PageManager.PageManager(DefaultSize, persistedStream, bp, lm, TestGlobals.TestFileLogger);

            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnInfo[] types);

            await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);
            await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);
            var p = await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);

            Assert.IsTrue(bp.GetAllDirtyPages().Any());

            rows.ForEach(r => p.Insert(r, tran));
            Assert.IsTrue(bp.GetAllDirtyPages().Any());

            await pageManager.Checkpoint();

            Assert.IsTrue(!bp.GetAllDirtyPages().Any());
        }

        [Test]
        [MaxTime(10000)]
        public async Task AttachAfterCheckpoint()
        {
            PersistedStream persistedStream = new PersistedStream(1024 * 1024, "checkpoint.data", createNew: true);
            IBufferPool bp = new BufferPool(TestGlobals.DefaultEviction, TestGlobals.DefaultPageSize);
            ILockManager lm = new LockManager.LockManager();
            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnInfo[] types);
            MixedPage p1, p2, p3;

            using (var pageManager = new PageManager.PageManager(DefaultSize, persistedStream, bp, lm, TestGlobals.TestFileLogger))
            {
                p1 = await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);
                p2 = await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);
                p3 = await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);

                rows.ForEach(r => p1.Insert(r, tran));
                rows.ForEach(r => p2.Insert(r, tran));
                rows.ForEach(r => p3.Insert(r, tran));

                await pageManager.Checkpoint();
            }

            PersistedStream persistedStream2 = new PersistedStream(1024 * 1024, "checkpoint.data", createNew: false);
            var eviction = new FifoEvictionPolicy(10, 5);
            using var pageManager2 =  new PageManager.PageManager(DefaultSize, eviction, persistedStream2);

            var readPage = await pageManager2.GetMixedPage(p1.PageId(), tran, types);
            Assert.IsTrue(p1.Equals(readPage, TestGlobals.DummyTran));
            readPage = await pageManager2.GetMixedPage(p2.PageId(), tran, types);
            Assert.IsTrue(p2.Equals(readPage, TestGlobals.DummyTran));
            readPage = await pageManager2.GetMixedPage(p3.PageId(), tran, types);
            Assert.IsTrue(p3.Equals(readPage, TestGlobals.DummyTran));
        }

        [Test]
        [MaxTime(10000)]
        public async Task CheckShadowPageValidity()
        {
            PersistedStream persistedStream = new PersistedStream(1024 * 1024, "checkpoint.data", createNew: true);
            IBufferPool bp = new BufferPool(TestGlobals.DefaultEviction, TestGlobals.DefaultPageSize);
            ILockManager lm = new LockManager.LockManager();
            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnInfo[] types);
            MixedPage p1, p2, p3;
            IPage shadowPage;

            using (var pageManager = new PageManager.PageManager(DefaultSize, persistedStream, bp, lm, TestGlobals.TestFileLogger))
            {
                // TODO: With buffer pool logging we will need to check that the transaction remains open.
                p1 = await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);
                p2 = await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);
                p3 = await pageManager.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran);

                // Checkpoint to flush allocation map.
                await pageManager.Checkpoint();

                rows.ForEach(r => p1.Insert(r, tran));
                rows.ForEach(r => p2.Insert(r, tran));
                rows.ForEach(r => p3.Insert(r, tran));

                await pageManager.Checkpoint();
                shadowPage = await persistedStream.GetShadowPage(PageType.MixedPage, DefaultSize, types);
            }

            // p3 is the last one.
            Assert.IsTrue(p3.Equals((MixedPage)shadowPage, tran));

            PersistedStream persistedStream2 = new PersistedStream(1024 * 1024, "checkpoint.data", createNew: false);

            IPage shadowPage2 = await persistedStream2.GetShadowPage(PageType.MixedPage, DefaultSize, types);
            Assert.IsTrue(((MixedPage)shadowPage).Equals((MixedPage)shadowPage2, tran));
        }
    }
}
