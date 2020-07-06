using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("LockManagerTests")]
namespace LockManager
{
    public class LockMonitor : ILockMonitor
    {
        private Dictionary<ulong, Dictionary<int, LockTypeEnum>> lockMonitorRecords = new Dictionary<ulong, Dictionary<int, LockTypeEnum>>();
        private object lck = new object();

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
                    Dictionary<int, LockTypeEnum> newSubDictionary = new Dictionary<int, LockTypeEnum>();
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

        private IEnumerable<int> GetLocksOfTypeForOwner(ulong ownerId, LockTypeEnum lockType)
        {
            foreach (KeyValuePair<int, LockTypeEnum> locks in this.lockMonitorRecords[ownerId])
            {
                if (locks.Value == lockType)
                {
                    yield return locks.Key;
                }
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

        protected void VerifyDeadlock(ulong ownerId, int lockId, LockTypeEnum lockType)
        {
            if (lockType == LockTypeEnum.Shared)
            {
                // Don't do deadlock detection for shared for now.
                return;
            }

            lock (lck)
            {
                foreach (KeyValuePair<ulong, Dictionary<int, LockTypeEnum>> ownersLocks in this.lockMonitorRecords)
                {
                    if (ownersLocks.Value.TryGetValue(lockId, out LockTypeEnum holderLockType))
                    {
                        // this lock is already taken. Need to check for deadlocks
                        if (ownersLocks.Key == ownerId)
                        {
                            throw new InvalidProgramException("No support for nested locks");
                        }

                        if (holderLockType == LockTypeEnum.Shared)
                        {
                            continue;
                        }

                        // Check intersection.
                        IEnumerable<int> requesterLocks = GetLocksOfTypeForOwner(ownerId, LockTypeEnum.Exclusive);
                        IEnumerable<int> ownerLocks = GetLocksOfTypeForOwner(ownersLocks.Key, LockTypeEnum.Exclusive);

                        if (requesterLocks.Intersect(ownerLocks).Any())
                        {
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
                return this.lockMonitorRecords[ownerId].Select(kv => (kv.Key, kv.Value)).ToArray();
            }
        }
    }
}
