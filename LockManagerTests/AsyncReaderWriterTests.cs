using NUnit.Framework;
using LockManager.LockImplementation;
using System.Threading.Tasks;
using System;

namespace LockManagerTests
{
    public class LockImplTests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public async Task AcquireReaders()
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1);
            using var reader = await lck.ReaderLockAsync();

            Assert.AreEqual(1, reader.LockId());
        }

        [Test]
        public async Task AcquireWriter()
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1);
            using var reader = await lck.WriterLockAsync();
        }

        [Test]
        public async Task MultiReader()
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1);
            Releaser[] rls = new Releaser[10];

            for (int i = 0; i < 10; i++)
            {
                rls[i] = await lck.ReaderLockAsync();
            }
        }

        [Test]
        public async Task MultiWriter()
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1);
            Releaser[] rls = new Releaser[2];

            rls[0] = await lck.WriterLockAsync();
            bool writerProceed = false;

            Task secondWriter = Task.Run(async () =>
            {
                rls[1] = await lck.WriterLockAsync(); writerProceed = true;
            });

            Assert.IsFalse(writerProceed);
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            Assert.IsFalse(writerProceed);
            rls[0].Dispose();
            secondWriter.Wait();
            Assert.IsTrue(writerProceed);
        }

        [Test]
        public async Task ReaderWriter()
        {
            AsyncReadWriterLock lck = new AsyncReadWriterLock(1);

            Releaser[] rls = new Releaser[2];

            rls[0] = await lck.ReaderLockAsync();
            bool writerProceed = false;

            Task secondWriter = Task.Run(async () =>
            {
                rls[1] = await lck.WriterLockAsync(); writerProceed = true;
            });

            Assert.IsFalse(writerProceed);
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            Assert.IsFalse(writerProceed);
            rls[0].Dispose();
            secondWriter.Wait();
            Assert.IsTrue(writerProceed);
        }
    }
}