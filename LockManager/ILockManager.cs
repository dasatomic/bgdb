using LockManager.LockImplementation;
using System.Threading.Tasks;

namespace LockManager
{
    public interface ILockManager
    {
        Task<Releaser> AcquireLock(LockTypeEnum lockType, ulong pageId);
        int LockIdForPage(ulong pageId);
    }
}
