using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LockManager.LockImplementation
{
    public class AsyncReadWriterLock
    {
        private readonly Queue<TaskCompletionSource<Releaser>> waitingWriters = new Queue<TaskCompletionSource<Releaser>>();
        private TaskCompletionSource<Releaser> waitingReader = new TaskCompletionSource<Releaser>();
        private int status;

        private readonly Task<Releaser> readerReleaser;
        private readonly Task<Releaser> writerReleaser;
        private int readersWaiting;

        private readonly int lockId;

        public AsyncReadWriterLock(int lockId)
        {
            readerReleaser = Task.FromResult(new Releaser(this, false, lockId));
            writerReleaser = Task.FromResult(new Releaser(this, true, lockId));

            this.lockId = lockId;
        }

        public Task<Releaser> ReaderLockAsync()
        {
            lock (waitingWriters)
            {
                if (status >= 0 && waitingWriters.Count == 0)
                {
                    ++status;
                    return readerReleaser;
                }
                else
                {
                    ++readersWaiting;
                    return waitingReader.Task.ContinueWith(t => t.Result);
                }
            }
        }
        public Task<Releaser> WriterLockAsync()
        {
            lock (waitingWriters)
            {
                if (status == 0)
                {
                    status = -1;
                    return writerReleaser;
                }
                else
                {
                    var waiter = new TaskCompletionSource<Releaser>();
                    waitingWriters.Enqueue(waiter);
                    return waiter.Task;
                }
            }
        }

        public void ReaderRelease()
        {
            TaskCompletionSource<Releaser> toWake = null;

            lock (waitingWriters)
            {
                --status;
                if (status == 0 && waitingWriters.Count > 0)
                {
                    status = -1;
                    toWake = waitingWriters.Dequeue();
                }
            }

            if (toWake != null)
            {
                toWake.SetResult(new Releaser(this, true, this.lockId));
            }
        }

        public void WriterRelease()
        {
            TaskCompletionSource<Releaser> toWake = null;
            bool toWakeIsWriter = false;

            lock (waitingWriters)
            {
                if (waitingWriters.Count > 0)
                {
                    toWake = waitingWriters.Dequeue();
                    toWakeIsWriter = true;
                }
                else if (readersWaiting > 0)
                {
                    toWake = waitingReader;
                    status = readersWaiting;
                    readersWaiting = 0;
                    waitingReader = new TaskCompletionSource<Releaser>();
                }
                else
                {
                    status = 0;
                }
            }

            if (toWake != null)
            {
                toWake.SetResult(new Releaser(this, toWakeIsWriter, this.lockId));
            }
        }
    }
}
