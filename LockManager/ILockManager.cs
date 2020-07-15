using LockManager.LockImplementation;
using System.Threading.Tasks;

namespace LockManager
{
    public interface ILockManager
    {
        Task<Releaser> AcquireLock(LockTypeEnum lockType, ulong pageId, ulong ownerId);
        int LockIdForPage(ulong pageId);
        public LockStats GetLockStats();
    }
}
