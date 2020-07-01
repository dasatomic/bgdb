using NUnit.Framework;
using LockManager.LockImplementation;
using System.Threading.Tasks;
using System;
using LockManager;
using System.Threading;

namespace LockManagerTests
{
    public class LockImplTests
    {
        private ILockMonitor lckmon = new LockMonitor();

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public async Task AcquireReaders()
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1, lckmon);
            using var reader = await lck.ReaderLockAsync(1);

            Assert.AreEqual(1, reader.LockId());
        }

        [Test]
        public async Task AcquireWriter()
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1, lckmon);
            using var reader = await lck.WriterLockAsync(1);
        }

        [Test]
        public async Task MultiReader()
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1, lckmon);
            Releaser[] rls = new Releaser[10];

            for (int i = 0; i < 10; i++)
            {
                rls[i] = await lck.ReaderLockAsync((ulong)i);
            }

            foreach (var r in rls)
            {
                r.Dispose();
            }
        }

        [Test]
        public async Task MultiWriter()
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1, lckmon);
            Releaser[] rls = new Releaser[2];

            rls[0] = await lck.WriterLockAsync(1);
            bool writerProceed = false;

            Task secondWriter = Task.Run(async () =>
            {
                rls[1] = await lck.WriterLockAsync(2); writerProceed = true;
            });

            Assert.IsFalse(writerProceed);
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            Assert.IsFalse(writerProceed);
            rls[0].Dispose();
            secondWriter.Wait();
            Assert.IsTrue(writerProceed);

            foreach (var r in rls)
            {
                r.Dispose();
            }
        }

        [Test]
        public async Task ReaderWriter()
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1, lckmon);

            Releaser[] rls = new Releaser[2];

            rls[0] = await lck.ReaderLockAsync(1);
            bool writerProceed = false;

            Task secondWriter = Task.Run(async () =>
            {
                rls[1] = await lck.WriterLockAsync(2); writerProceed = true;
            });

            Assert.IsFalse(writerProceed);
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            Assert.IsFalse(writerProceed);
            rls[0].Dispose();
            secondWriter.Wait();
            Assert.IsTrue(writerProceed);

            foreach (var r in rls)
            {
                r.Dispose();
            }
        }

        [Test, Pairwise]
        public async Task LockCombinations(
            [Values(LockTypeEnum.Shared, LockTypeEnum.Exclusive)] LockTypeEnum lckType1,
            [Values(LockTypeEnum.Shared, LockTypeEnum.Exclusive)] LockTypeEnum lckType2,
            [Values(LockTypeEnum.Shared, LockTypeEnum.Exclusive)] LockTypeEnum lckType3,
            [Values(LockTypeEnum.Shared, LockTypeEnum.Exclusive)] LockTypeEnum lckType4)
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1, lckmon);
            Releaser[] rls = new Releaser[4];
            AutoResetEvent[] evts = new AutoResetEvent[4];

            for (int i = 0; i < evts.Length; i++)
            {
                evts[i] = new AutoResetEvent(false);
            }

            Task t0 = Task.Run(async () =>
            {
                if (lckType1 == LockTypeEnum.Shared)
                {
                    rls[0] = await lck.ReaderLockAsync(1);
                }
                else
                {
                    rls[0] = await lck.WriterLockAsync(1);
                }

                evts[0].WaitOne();

                rls[0].Dispose();
            });

            Task t1 = Task.Run(async () =>
            {
                if (lckType2 == LockTypeEnum.Shared)
                {
                    rls[1] = await lck.ReaderLockAsync(2);
                }
                else
                {
                    rls[1] = await lck.WriterLockAsync(2);
                }

                evts[1].WaitOne();

                rls[1].Dispose();
            });

            Task t2 = Task.Run(async () =>
            {
                if (lckType3 == LockTypeEnum.Shared)
                {
                    rls[2] = await lck.ReaderLockAsync(3);
                }
                else
                {
                    rls[2] = await lck.WriterLockAsync(3);
                }

                evts[2].WaitOne();

                rls[2].Dispose();
            });

            Task t3 = Task.Run(async () =>
            {
                if (lckType4 == LockTypeEnum.Shared)
                {
                    rls[3] = await lck.ReaderLockAsync(4);
                }
                else
                {
                    rls[3] = await lck.WriterLockAsync(4);
                }

                evts[3].WaitOne();

                rls[3].Dispose();
            });

            await Task.Delay(100);

            evts[0].Set();
            evts[1].Set();
            evts[2].Set();
            evts[3].Set();

            Assert.IsTrue(Task.WaitAll(new Task[] { t0, t1, t2, t3 }, TimeSpan.FromSeconds(5)));
        }
    }
}