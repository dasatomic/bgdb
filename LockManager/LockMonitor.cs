using System.Collections.Generic;

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
    }
}
