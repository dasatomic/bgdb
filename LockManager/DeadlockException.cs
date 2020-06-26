using System;

namespace LockManager
{
    public class DeadlockException : Exception
    {
        private ulong victimId;
        private ulong ownerId;
        private int lockId;

        public DeadlockException(ulong victimId, ulong ownerId, int lockId)
        {
            this.victimId = victimId;
            this.ownerId = ownerId;
            this.lockId = lockId;
        }
    }
}
