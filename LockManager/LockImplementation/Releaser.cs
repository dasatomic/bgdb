using System;

namespace LockManager.LockImplementation
{
    public struct Releaser : IDisposable
    {
        private readonly AsyncReadWriterLock toRelease;
        private readonly bool writer;
        private readonly int lockId;
        private readonly ulong ownerId;
        private Action releaseCallback;

        internal Releaser(AsyncReadWriterLock toRelease, bool writer, int lockId, ulong ownerId)
        {
            this.toRelease = toRelease;
            this.writer = writer;
            this.lockId = lockId;
            this.ownerId = ownerId;
            releaseCallback = null;
        }

        internal bool IsWriter() => writer;

        public void SetReleaseCallback(Action callback)
        {
            this.releaseCallback = callback;
        }

        public int LockId() => lockId;

        public void Dispose()
        {
            if (toRelease != null)
            {
                if (releaseCallback != null)
                {
                    releaseCallback();
                }

                if (writer) toRelease.WriterRelease(this.ownerId);
                else toRelease.ReaderRelease(this.ownerId);
            }
        }
    }
}
