using LockManager;
using NUnit.Framework;
using PageManager;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Test.Common;

namespace PageManagerTests
{
    public class ConcurrentPageAccessTests
    {
        private const int DefaultSize = 4096;
        private const ulong DefaultPrevPage = PageManagerConstants.NullPageId;
        private const ulong DefaultNextPage = PageManagerConstants.NullPageId;

        [Test, MaxTime(120000)]
        // [Repeat(1000)]
        public async Task ConcurrentWriteTests()
        {
            PersistedStream persistedStream = new PersistedStream(1024 * 1024, "concurrent.data", createNew: true, TestGlobals.TestFileLogger);
            IPageEvictionPolicy evictionPolicy = new FifoEvictionPolicy(1, 1);
            IBufferPool bp = new BufferPool(evictionPolicy, TestGlobals.DefaultPageSize);
            ILockManager lm = new LockManager.LockManager();
            var lgm = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            using var pm =  new PageManager.PageManager(DefaultSize, evictionPolicy, persistedStream, bp, lm, TestGlobals.TestFileLogger);

            const int workerCount = 50;

            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnInfo[] types);

            async Task generatePagesAction()
            {
                for (int i = 0; i < workerCount; i++)
                {
                    using (ITransaction tran = lgm.CreateTransaction(pm))
                    {
                        var mp = await pm.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran).ConfigureAwait(false);
                        await tran.AcquireLock(mp.PageId(), LockTypeEnum.Exclusive).ConfigureAwait(false);
                        rows.ForEach(r => mp.Insert(r, tran));
                        await tran.Commit().ConfigureAwait(false);
                    }
                }
            }

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < workerCount; i++)
            {
                tasks.Add(generatePagesAction());
            }

            Task allDoneTask = Task.WhenAll(tasks);
            Task winner = await Task.WhenAny(allDoneTask, Task.Delay(TimeSpan.FromMinutes(2)));

            Assert.IsTrue(winner == allDoneTask);
        }

        [Test, MaxTime(120000)]
        // [Repeat(1000)]
        public async Task ConcurrentReadAndWriteTests()
        {
            PersistedStream persistedStream = new PersistedStream(1024 * 1024, "concurrent.data", createNew: true, TestGlobals.TestFileLogger);
            IPageEvictionPolicy evictionPolicy = new FifoEvictionPolicy(1, 1);
            IBufferPool bp = new BufferPool(evictionPolicy, TestGlobals.DefaultPageSize);
            ILockManager lm = new LockManager.LockManager();
            var lgm = new LogManager.LogManager(new BinaryWriter(new MemoryStream()));
            using var pm =  new PageManager.PageManager(DefaultSize, evictionPolicy, persistedStream, bp, lm, TestGlobals.TestFileLogger);

            long maxPageId = 0;

            const int workerCount = 50;

            var rows = GenerateDataUtils.GenerateRowsWithSampleData(out ColumnInfo[] types);

            async Task generatePagesAction()
            {
                for (int i = 0; i < workerCount; i++)
                {
                    using (ITransaction tran = lgm.CreateTransaction(pm))
                    {
                        try
                        {
                            var mp = await pm.AllocateMixedPage(types, DefaultPrevPage, DefaultNextPage, tran).ConfigureAwait(false);
                            await tran.AcquireLock(mp.PageId(), LockTypeEnum.Exclusive).ConfigureAwait(false);
                            rows.ForEach(r => mp.Insert(r, tran));
                            await tran.Commit().ConfigureAwait(false);
                            Interlocked.Exchange(ref maxPageId, (long)mp.PageId());
                        }
                        catch (DeadlockException)
                        {
                            await tran.Rollback().ConfigureAwait(false);
                        }
                    }
                }
            }

            async Task readRandomPages()
            {
                for (int i = 0; i < workerCount; i++)
                {
                    using (ITransaction tran = lgm.CreateTransaction(pm))
                    {
                        long currMaxPageId = Interlocked.Read(ref maxPageId);

                        if (maxPageId < 3)
                        {
                            continue;
                        }

                        Random rnd = new Random();
                        ulong pageToRead = (ulong)rnd.Next(3, (int)currMaxPageId);

                        using (var _ = await tran.AcquireLock(pageToRead, LockTypeEnum.Shared).ConfigureAwait(false))
                        {
                            try
                            {
                                await pm.GetMixedPage(pageToRead, tran, types).ConfigureAwait(false);
                            }
                            catch (DeadlockException)
                            {
                                await tran.Rollback().ConfigureAwait(false);
                            }
                        }

                        await tran.Commit().ConfigureAwait(false);
                    }
                }
            }

            List<Task> tasks = new List<Task>();
            for (int i = 0; i < workerCount; i++)
            {
                tasks.Add(generatePagesAction());
                tasks.Add(readRandomPages());
            }

            Task allDoneTask = Task.WhenAll(tasks);
            Task winner = await Task.WhenAny(allDoneTask, Task.Delay(TimeSpan.FromMinutes(2)));

            if (winner != allDoneTask)
            {
                var stats = lm.GetActiveLocks();

                foreach (var stat in stats)
                {
                    TestContext.Progress.WriteLine($"Owner - {stat.ownerId}, on lock - {stat.lockId}, type - {stat.lockType}");
                }
            }

            Assert.IsTrue(winner == allDoneTask);
        }
    }
}
