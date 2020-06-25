using NUnit.Framework;
using LockManager.LockImplementation;
using System.Threading.Tasks;
using System;
using LockManager;

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
    }
}