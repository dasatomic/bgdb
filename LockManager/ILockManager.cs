using LockManager.LockImplementation;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace LockManager
{
    public interface ILockManager
    {
        Task<Releaser> AcquireLock(LockTypeEnum lockType, ulong pageId, ulong ownerId);
        int LockIdForPage(ulong pageId);
        public LockStats GetLockStats();
        public IEnumerable<LockMonitorRecord> GetActiveLocks();

        /// <summary>
        /// Once transaction (owner) is completed it should tell lock manager to
        /// clean up any extra tracking for that transaction.
        /// </summary>
        /// <param name="ownerId"></param>
        public void ReleaseOwner(ulong ownerId);
    }
}
