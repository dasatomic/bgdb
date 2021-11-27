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
        private bool isDisposed;

        internal Releaser(AsyncReadWriterLock toRelease, bool writer, int lockId, ulong ownerId)
        {
            this.toRelease = toRelease;
            this.writer = writer;
            this.lockId = lockId;
            this.ownerId = ownerId;
            this.releaseCallback = null;
            this.lockAcquisitionStart = DateTime.UtcNow;
            this.isDisposed = false;
        }

        public static Releaser FakeReleaser
        {
            get
            {
                return new Releaser(null, false, 0, 0);
            }
        }

        internal bool IsWriter() => writer;

        public void SetReleaseCallback(Action callback)
        {
            this.releaseCallback = callback;
        }

        public int LockId() => lockId;

        public void Dispose()
        {
            if (!isDisposed)
            {
                releaseCallback?.Invoke();

                if (toRelease != null)
                {
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

                isDisposed = true;
            }
        }
    }
}
