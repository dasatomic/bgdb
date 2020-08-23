using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;

[assembly: InternalsVisibleTo("LockManagerTests")]
namespace LockManager
{
    public class LockMonitor : ILockMonitor
    {
        private Dictionary<ulong /* owner id */, Dictionary<int /* lock id */, LockTypeEnum>> lockMonitorRecords = new Dictionary<ulong, Dictionary<int, LockTypeEnum>>();
        private object lck = new object();

        // TODO: This should be a ring buffer.
        private List<LockStatsRecord> lockStats = new List<LockStatsRecord>();

        public void AddRecord(LockMonitorRecord record)
        {
            lock (lck)
            {
                if (lockMonitorRecords.TryGetValue(record.ownerId, out Dictionary<int, LockTypeEnum> subdictionary))
                {
                    CheckRecursiveLock(record.ownerId, record.lockId, record.lockType);
                    VerifyDeadlock(record.ownerId, record.lockId, record.lockType);
                    subdictionary.Add(record.lockId, record.lockType);
                }
                else
                {
                    var newSubDictionary = new Dictionary<int, LockTypeEnum>();
                    newSubDictionary.Add(record.lockId, record.lockType);
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
            this.lockMonitorRecords.TryGetValue(ownerId, out Dictionary<int, LockTypeEnum> myLocks);

            if (myLocks != null)
            {
                if (myLocks.ContainsKey(lockId))
                {
                    throw new RecursiveLockNotSupportedException();
                }
            }
        }

        protected void VerifyDeadlock(ulong reqOwnerId, int reqLockId, LockTypeEnum reqLockType)
        {
            // TODO: This needs to be optimized.
            // Only aiming for correctness here.
            lock (lck)
            {
                Dictionary<int, LockTypeEnum> locksVisited = new Dictionary<int, LockTypeEnum>();
                Queue<(int, LockTypeEnum)> locksToCheck = new Queue<(int, LockTypeEnum)>();

                {
                    Dictionary<int, LockTypeEnum> lockToCheckBuilder = new Dictionary<int, LockTypeEnum>();
                    foreach ((ulong depOwnerId, Dictionary<int, LockTypeEnum> depLocks) in this.lockMonitorRecords)
                    {
                        if (depLocks.ContainsKey(reqLockId))
                        {
                            foreach ((int lockId, LockTypeEnum lockType) in depLocks)
                            {
                                if (lockId != reqLockId)
                                {
                                    if (!(lockType == LockTypeEnum.Shared && reqLockType == LockTypeEnum.Shared))
                                    {
                                        if (lockToCheckBuilder.TryGetValue(lockId, out LockTypeEnum insertedLockType))
                                        {
                                            if (insertedLockType < lockType)
                                            {
                                                lockToCheckBuilder[lockId] = insertedLockType;
                                            }
                                        }
                                        else
                                        {
                                            lockToCheckBuilder.Add(lockId, lockType);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    foreach (var lck in lockToCheckBuilder)
                    {
                        locksToCheck.Enqueue((lck.Key, lck.Value));
                    }
                }

                Debug.Assert(!locksVisited.Any());

                while (locksToCheck.Any())
                {
                    (int lockToCheck, LockTypeEnum lockTypeToCheck) = locksToCheck.Dequeue();

                    foreach ((ulong depOwnerId, Dictionary<int, LockTypeEnum> depLocks) in this.lockMonitorRecords)
                    {
                        if (depLocks.ContainsKey(lockToCheck))
                        {
                            if (depOwnerId == reqOwnerId)
                            {
                                // We came back to the original owner. This means that this is a deadlock.
                                // TODO: This doesn't take into account lock type.
                                // E.g., even for a circle of readonly locks it will detect cycle.
                                throw new DeadlockException(reqOwnerId, depOwnerId, reqLockId);
                            }

                            // Find all the locks that haven't been in the to visit list so far.
                            foreach ((int depLockId, LockTypeEnum depLockType) in depLocks)
                            {
                                if (locksVisited.TryGetValue(depLockId, out LockTypeEnum visitedLockType))
                                {
                                    // If this is an upgrade I will need to visit it.
                                    if (visitedLockType < depLockType)
                                    {
                                        locksToCheck.Enqueue((depLockId, depLockType));
                                        locksVisited[depLockId] = depLockType;
                                    }
                                }
                                else
                                {
                                    locksToCheck.Enqueue((depLockId, depLockType));
                                    locksVisited.Add(depLockId, depLockType);
                                }
                            }
                        }
                    }
                }

                return;
            }
        }

        internal (int, LockTypeEnum)[] GetSnapshot(ulong ownerId)
        {
            lock (lck)
            {
                return this.lockMonitorRecords[ownerId].Select(kv => (kv.Key, kv.Value)).ToArray();
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

        public IEnumerable<LockMonitorRecord> GetActiveLocks()
        {
            lock (lck)
            {
                foreach (var record in this.lockMonitorRecords)
                {
                    foreach (var entry in record.Value)
                    {
                        yield return new LockMonitorRecord(record.Key, entry.Key, entry.Value);
                    }
                }
            }
        }
    }
}
