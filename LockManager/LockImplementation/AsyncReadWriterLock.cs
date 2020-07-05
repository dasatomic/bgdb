using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace LockManager.LockImplementation
{
    public class AsyncReadWriterLock
    {
        private readonly Queue<(ulong, DateTime entryTime, TaskCompletionSource<Releaser>)> waitingWriters = new Queue<(ulong, DateTime, TaskCompletionSource<Releaser>)>();
        private Queue<(ulong, DateTime entryTime, TaskCompletionSource<Releaser>)> waitingReaders = new Queue<(ulong, DateTime, TaskCompletionSource<Releaser>)>();
        private int status;

        private readonly ILockMonitor lockMonitor;

        private readonly LockManagerInstrumentationInterface logger;

        private int readersWaiting;

        private readonly int lockId;

        public AsyncReadWriterLock(int lockId, ILockMonitor lockMonitor) : this(lockId, lockMonitor, new NoOpLogging()) { }

        public AsyncReadWriterLock(int lockId, ILockMonitor lockMonitor, LockManagerInstrumentationInterface logger)
        {
            this.lockMonitor = lockMonitor;
            this.lockId = lockId;
            this.logger = logger;
        }

        public Task<Releaser> ReaderLockAsync(ulong ownerId)
        {
            lock (waitingWriters)
            {
                this.lockMonitor.AddRecord(new LockMonitorRecord(ownerId, this.lockId, LockTypeEnum.Shared));
                if (status >= 0 && waitingWriters.Count == 0)
                {
                    this.logger.LogDebug($"Owner {ownerId} taking lockId {this.lockId} as Shared. Active readers {status}.");
                    ++status;
                    return Task.FromResult(new Releaser(this, false, this.lockId, ownerId));
                }
                else
                {
                    this.logger.LogDebug($"Owner {ownerId} waiting for lockId {this.lockId} as Shared. Readers already waiting {readersWaiting}.");
                    ++readersWaiting;
                    var reader = new TaskCompletionSource<Releaser>();
                    waitingReaders.Enqueue((ownerId, DateTime.UtcNow, reader));
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
                    this.logger.LogDebug($"Owner {ownerId} taking lockId {this.lockId} as Exclusive.");
                    status = -1;
                    return Task.FromResult(new Releaser(this, true, this.lockId, ownerId));
                }
                else
                {
                    this.logger.LogDebug($"Owner {ownerId} waiting for lockId {this.lockId} as Exclusive. Readers already waiting {this.readersWaiting}. Writers waiting {this.waitingWriters.Count }");
                    var waiter = new TaskCompletionSource<Releaser>();
                    waitingWriters.Enqueue((ownerId, DateTime.UtcNow, waiter));
                    return waiter.Task;
                }
            }
        }

        public void ReaderRelease(ulong ownerId, TimeSpan timeHeld)
        {
            TaskCompletionSource<Releaser> toWake = null;
            ulong toWakeId = 0;

            lock (waitingWriters)
            {
                this.logger.LogDebug($"Owner {ownerId} releasing reader lock {this.lockId} after {timeHeld.TotalMilliseconds}ms.");

                this.lockMonitor.ReleaseRecord(ownerId, this.lockId);
                --status;
                if (status == 0 && waitingWriters.Count > 0)
                {
                    status = -1;
                    DateTime? toWakeWaitingStart = null;
                    (toWakeId, toWakeWaitingStart, toWake) = waitingWriters.Dequeue();

                    TimeSpan waitingTime = DateTime.UtcNow - toWakeWaitingStart.Value;
                    this.logger.LogDebug($"Owner {ownerId} releasing reader lock {this.lockId}. Waking writer {toWakeId} that waited for {waitingTime.TotalMilliseconds}ms");
                }

                if (toWake != null)
                {
                    toWake.SetResult(new Releaser(this, true, this.lockId, toWakeId));
                }
            }
        }

        public void WriterRelease(ulong ownerId, TimeSpan timeHeld)
        {
            List<(ulong, DateTime, TaskCompletionSource<Releaser>)> toWake = new List<(ulong, DateTime, TaskCompletionSource<Releaser>)>();
            bool toWakeIsWriter = false;

            lock (waitingWriters)
            {
                this.logger.LogDebug($"Owner {ownerId} releasing writer lock {this.lockId} after {timeHeld.TotalMilliseconds}ms.");
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

                foreach ((ulong nextOwnerId, DateTime waitingStart, TaskCompletionSource<Releaser>taskToWait) in toWake)
                {
                    TimeSpan waitingTime = DateTime.UtcNow - waitingStart;
                    if (toWakeIsWriter)
                    {
                        this.logger.LogDebug($"Owner {ownerId} releasing writer lock {this.lockId}. Waking writer {nextOwnerId} that waited for {waitingTime.TotalMilliseconds}ms");
                    }
                    else
                    {
                        this.logger.LogDebug($"Owner {ownerId} releasing writer lock {this.lockId}. Waking reader {nextOwnerId} that waited for {waitingTime.TotalMilliseconds}ms");
                    }

                    taskToWait.SetResult(new Releaser(this, toWakeIsWriter, this.lockId, nextOwnerId));
                }
            }
        }
    }
}
