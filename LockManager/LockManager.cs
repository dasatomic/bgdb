using LockManager.LockImplementation;
using System;
using System.Threading.Tasks;

namespace LockManager
{
    public class LockManager : ILockManager
    {

        private static readonly int[] numOfLocksRange = { 769, 1543, 3079, 6151, 12289, 24593, 49157 };
        private AsyncReadWriterLock[] locks;
        private readonly ILockMonitor lockMonitor;
        private readonly LockManagerInstrumentationInterface logger;

        public LockManager() : this(new LockMonitor(), new NoOpLogging()) { }

        public LockManager(ILockMonitor lockMonitor, LockManagerInstrumentationInterface logger)
        {
            this.lockMonitor = lockMonitor;
            int numOfLocks = numOfLocksRange[0];
            this.locks = new AsyncReadWriterLock[numOfLocks];
            this.logger = logger;

            for (int i = 0; i < numOfLocks; i++)
            {
                this.locks[i] = new AsyncReadWriterLock(i, lockMonitor, this.logger);
            }

            logger.LogInfo($"Starting lock manager with {numOfLocks} pre-created locks");
        }

        public async Task<Releaser> AcquireLock(LockTypeEnum lockType, ulong pageId, ulong ownerId)
        {
            return lockType switch
            {
                LockTypeEnum.Shared => await this.locks[pageId % (ulong)locks.Length].ReaderLockAsync(ownerId),
                LockTypeEnum.Exclusive => await this.locks[pageId % (ulong)locks.Length].WriterLockAsync(ownerId),
                _ => throw new ArgumentException()
            };
        }

        public int LockIdForPage(ulong pageId) => (int)(pageId % (ulong)locks.Length);
    }
}
