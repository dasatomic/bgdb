using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;

[assembly: InternalsVisibleTo("LockManagerTests")]
namespace LockManager
{
    public class LockMonitor : ILockMonitor
    {
        private Dictionary<ulong, Dictionary<int, (LockTypeEnum, ulong)>> lockMonitorRecords = new Dictionary<ulong, Dictionary<int, (LockTypeEnum, ulong)>>();
        private object lck = new object();

        // TODO: This should be a ring buffer.
        private List<LockStatsRecord> lockStats = new List<LockStatsRecord>();
        private ulong timestampId;

        public void AddRecord(LockMonitorRecord record)
        {
            lock (lck)
            {
                if (lockMonitorRecords.TryGetValue(record.ownerId, out Dictionary<int, (LockTypeEnum, ulong)> subdictionary))
                {
                    CheckRecursiveLock(record.ownerId, record.lockId, record.lockType);
                    VerifyDeadlock(record.ownerId, record.lockId, record.lockType);
                    subdictionary.Add(record.lockId, (record.lockType, timestampId++));
                }
                else
                {
                    var newSubDictionary = new Dictionary<int, (LockTypeEnum, ulong)>();
                    newSubDictionary.Add(record.lockId, (record.lockType, timestampId++));
                    lockMonitorRecords.Add(record.ownerId, newSubDictionary);
                }
            }
        }

        public void ReleaseRecord(ulong ownerId, int lockId)
        {
            lock (lck)
            {
                lockMonitorRecords[ownerId].Remove(lockId);
            }
        }

        protected void CheckRecursiveLock(ulong ownerId, int lockId, LockTypeEnum lockType)
        {
            this.lockMonitorRecords.TryGetValue(ownerId, out Dictionary<int, (LockTypeEnum, ulong)> myLocks);

            if (myLocks != null)
            {
                if (myLocks.ContainsKey(lockId))
                {
                    throw new RecursiveLockNotSupportedException();
                }
            }
        }

        protected void VerifyDeadlock(ulong ownerId, int lockId, LockTypeEnum lockType)
        {
            lock (lck)
            {
                Dictionary<int, (LockTypeEnum, ulong)> requesterLocks = this.lockMonitorRecords[ownerId];

                foreach (KeyValuePair<ulong, Dictionary<int, (LockTypeEnum, ulong)>> ownersLocks in this.lockMonitorRecords)
                {
                    if (ownersLocks.Value.TryGetValue(lockId, out (LockTypeEnum holderLockType, ulong holderTimestamp) holder))
                    {
                        // this lock is already taken. Need to check for deadlocks
                        if (ownersLocks.Key == ownerId)
                        {
                            throw new InvalidProgramException("No support for nested locks");
                        }

                        // Check if locks are compatible.
                        if (lockType == LockTypeEnum.Shared && holder.holderLockType == LockTypeEnum.Shared)
                        {
                            // Both can proceed. This is fine.
                            continue;
                        }

                        // check if this guy depends on any locks that I am holding.

                        // Check intersection.
                        Dictionary<int, (LockTypeEnum, ulong)> ownerLocks = this.lockMonitorRecords[ownersLocks.Key];

                        foreach (int intersectionLockId in requesterLocks.Keys.Intersect(ownerLocks.Keys))
                        {
                            if (requesterLocks[intersectionLockId].Item1 == LockTypeEnum.Shared && ownerLocks[intersectionLockId].Item1 == LockTypeEnum.Shared)
                            {
                                // Again, we are not mutually blocking.
                                continue;
                            }

                            // TODO: Need to check timestamps here as well...

                            // This must be RW combination.

                            /*
                            if (lockType == LockTypeEnum.Shared && requesterLocks[intersectionLockId].Item1 == LockTypeEnum.Shared)
                            {
                                continue;
                            }

                            if (holder.holderLockType == LockTypeEnum.Shared && ownerLocks[intersectionLockId].Item1 == LockTypeEnum.Shared)
                            {
                                continue;
                            }
                            */

                            // TODO: Even timestamp is not enough...
                            // Lock itself needs to build a graph...

                            // This is a deadlock.
                            throw new DeadlockException(ownerId, ownersLocks.Key, lockId);
                        }
                    }
                }
            }
        }

        internal (int, LockTypeEnum)[] GetSnapshot(ulong ownerId)
        {
            lock (lck)
            {
                return this.lockMonitorRecords[ownerId].Select(kv => (kv.Key, kv.Value.Item1)).ToArray();
            }
        }

        public void RecordStats(LockStatsRecord statsRecord)
        {
            // TODO: Locking can be done better + now stats grow indefinitely.
            lock (lck)
            {
                this.lockStats.Add(statsRecord);
            }
        }

        private TimeSpan[] Percentile(IEnumerable<TimeSpan> seq, double[] percentiles)
        {
            var elements = seq.ToArray();
            Array.Sort(elements);
            TimeSpan[] res = new TimeSpan[percentiles.Length];
            int cnt = 0;

            foreach (double percentile in percentiles)
            {
                double realIndex = percentile * (elements.Length - 1);
                int index = (int)realIndex;
                double frac = realIndex - index;
                if (index + 1 < elements.Length)
                    res[cnt] = elements[index] * (1 - frac) + elements[index + 1] * frac;
                else
                    res[cnt] = elements[index];

                cnt++;
            }

            return res;
        }

        public LockStats GetStats()
        {
            lock (lck)
            {
                TimeSpan[] percentiles = Percentile(this.lockStats.Select(x => x.WaitDuration), new double[] { 0.5, 0.95, 0.99, 1 });
                return new LockStats
                {
                    WaitTimePercentile50th = percentiles[0],
                    WaitTimePercentile95th = percentiles[1],
                    WaitTimePercentile99th = percentiles[2],
                    WaitTimePercentileMax = percentiles[3],
                };
            }
        }
    }
}
