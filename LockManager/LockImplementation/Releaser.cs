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
        private readonly DateTime lockAcquisitionStart;

        internal Releaser(AsyncReadWriterLock toRelease, bool writer, int lockId, ulong ownerId)
        {
            this.toRelease = toRelease;
            this.writer = writer;
            this.lockId = lockId;
            this.ownerId = ownerId;
            releaseCallback = null;
            this.lockAcquisitionStart = DateTime.UtcNow;
        }

        internal bool IsWriter() => writer;

        public void SetReleaseCallback(Action callback)
        {
            this.releaseCallback = callback;
        }

        public int LockId() => lockId;

        public void Dispose()
        {
            if (releaseCallback != null)
            {
                releaseCallback();
            }

            TimeSpan duration = DateTime.UtcNow - lockAcquisitionStart;

            if (writer)
            {
                toRelease.WriterRelease(this.ownerId, duration);
            }
            else
            {
                toRelease.ReaderRelease(this.ownerId, duration);
            }
        }
    }
}
