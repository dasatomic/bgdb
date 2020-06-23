using System;

namespace LockManager.LockImplementation
{
    public struct Releaser : IDisposable
    {
        private readonly AsyncReadWriterLock toRelease;
        private readonly bool writer;
        private readonly int lockId;

        internal Releaser(AsyncReadWriterLock toRelease, bool writer, int lockId)
        {
            this.toRelease = toRelease;
            this.writer = writer;
            this.lockId = lockId;
        }

        internal bool IsWriter() => writer;

        public int LockId() => lockId;

        public void Dispose()
        {
            if (toRelease != null)
            {
                if (writer) toRelease.WriterRelease();
                else toRelease.ReaderRelease();
            }
        }
    }
}
