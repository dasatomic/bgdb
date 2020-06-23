using LockManager.LockImplementation;
using System;
using System.Threading.Tasks;

namespace LockManager
{
    public class LockManager : ILockManager
    {
        private static readonly int[] numOfLocksRange = { 769, 1543, 3079, 6151, 12289, 24593, 49157 };
        private AsyncReadWriterLock[] locks;

        public LockManager()
        {
            int numOfLocks = numOfLocksRange[0];
            this.locks = new AsyncReadWriterLock[numOfLocks];

            for (int i = 0; i < numOfLocks; i++)
            {
                this.locks[i] = new AsyncReadWriterLock(i);
            }
        }

        public async Task<Releaser> AcquireLock(LockTypeEnum lockType, ulong pageId)
        {
            return lockType switch
            {
                LockTypeEnum.Shared => await this.locks[pageId % (ulong)locks.Length].ReaderLockAsync(),
                LockTypeEnum.Exclusive => await this.locks[pageId % (ulong)locks.Length].WriterLockAsync(),
                _ => throw new ArgumentException()
            };
        }

        public int LockIdForPage(ulong pageId) => (int)(pageId % (ulong)locks.Length);
    }
}
