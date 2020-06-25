using System;
using System.Collections.Generic;
using System.Text;

namespace LockManager
{
    public struct LockMonitorRecord
    {
        public readonly ulong ownerId;
        public readonly int lockId;
        public readonly LockTypeEnum lockType;

        public LockMonitorRecord(ulong ownerId, int lockId, LockTypeEnum lockType)
        {
            this.ownerId = ownerId;
            this.lockId = lockId;
            this.lockType = lockType;
        }
    }

    public interface ILockMonitor
    {
        public void AddRecord(LockMonitorRecord record);
        public void ReleaseRecord(ulong ownerId, int lockId);
    }
}
