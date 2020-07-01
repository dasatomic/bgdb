using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace LockManager.LockImplementation
{
    public class AsyncReadWriterLock
    {
        private readonly Queue<(ulong, TaskCompletionSource<Releaser>)> waitingWriters = new Queue<(ulong, TaskCompletionSource<Releaser>)>();
        private Queue<(ulong, TaskCompletionSource<Releaser>)> waitingReaders = new Queue<(ulong, TaskCompletionSource<Releaser>)>();
        private int status;

        private readonly ILockMonitor lockMonitor;

        private int readersWaiting;

        private readonly int lockId;

        public AsyncReadWriterLock(int lockId, ILockMonitor lockMonitor)
        {
            this.lockMonitor = lockMonitor;

            this.lockId = lockId;
        }

        public Task<Releaser> ReaderLockAsync(ulong ownerId)
        {
            lock (waitingWriters)
            {
                this.lockMonitor.AddRecord(new LockMonitorRecord(ownerId, this.lockId, LockTypeEnum.Shared));
                if (status >= 0 && waitingWriters.Count == 0)
                {
                    ++status;
                    return Task.FromResult(new Releaser(this, false, this.lockId, ownerId));
                }
                else
                {
                    ++readersWaiting;
                    var reader = new TaskCompletionSource<Releaser>();
                    waitingReaders.Enqueue((ownerId, reader));
                    return reader.Task;
                }
            }
        }
        public Task<Releaser> WriterLockAsync(ulong ownerId)
        {
            lock (waitingWriters)
            {
                this.lockMonitor.AddRecord(new LockMonitorRecord(ownerId, this.lockId, LockTypeEnum.Exclusive));
                if (status == 0)
                {
                    status = -1;
                    return Task.FromResult(new Releaser(this, true, this.lockId, ownerId));
                }
                else
                {
                    var waiter = new TaskCompletionSource<Releaser>();
                    waitingWriters.Enqueue((ownerId, waiter));
                    return waiter.Task;
                }
            }
        }

        public void ReaderRelease(ulong ownerId)
        {
            TaskCompletionSource<Releaser> toWake = null;
            ulong toWakeId = 0;

            lock (waitingWriters)
            {
                this.lockMonitor.ReleaseRecord(ownerId, this.lockId);
                --status;
                if (status == 0 && waitingWriters.Count > 0)
                {
                    status = -1;
                    (toWakeId, toWake) = waitingWriters.Dequeue();
                }

                if (toWake != null)
                {
                    toWake.SetResult(new Releaser(this, true, this.lockId, toWakeId));
                }
            }
        }

        public void WriterRelease(ulong ownerId)
        {
            List<(ulong, TaskCompletionSource<Releaser>)> toWake = new List<(ulong, TaskCompletionSource<Releaser>)>();
            bool toWakeIsWriter = false;

            lock (waitingWriters)
            {
                this.lockMonitor.ReleaseRecord(ownerId, this.lockId);
                if (waitingWriters.Count > 0)
                {
                    status = -1;
                    toWake.Add(waitingWriters.Dequeue());
                    toWakeIsWriter = true;
                }
                else if (readersWaiting > 0)
                {
                    Debug.Assert(readersWaiting == waitingReaders.Count);

                    while (waitingReaders.Count != 0)
                    {
                        toWake.Add(waitingReaders.Dequeue());
                    }

                    status = readersWaiting;
                    readersWaiting = 0;
                }
                else
                {
                    status = 0;
                }

                foreach ((ulong nextOwnerId, TaskCompletionSource<Releaser>taskToWait) in toWake)
                {
                    taskToWait.SetResult(new Releaser(this, toWakeIsWriter, this.lockId, nextOwnerId));
                }
            }
        }
    }
}
