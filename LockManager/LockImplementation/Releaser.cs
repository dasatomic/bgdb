using System;

namespace LockManager.LockImplementation
{
    public struct Releaser : IDisposable
    {
        private readonly AsyncReadWriterLock toRelease;
        private readonly bool writer;
        private readonly int lockId;
        private Action releaseCallback;

        internal Releaser(AsyncReadWriterLock toRelease, bool writer, int lockId)
        {
            this.toRelease = toRelease;
            this.writer = writer;
            this.lockId = lockId;
            releaseCallback = null;
        }

        internal bool IsWriter() => writer;

        public int LockId() => lockId;

        public void SetReleaseCallback(Action callback)
        {
            this.releaseCallback = callback;
        }

        public void Dispose()
        {
            if (toRelease != null)
            {
                if (releaseCallback != null)
                {
                    releaseCallback();
                }

                if (writer) toRelease.WriterRelease();
                else toRelease.ReaderRelease();
            }
        }
    }
}
