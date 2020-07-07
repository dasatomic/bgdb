using LockManager;
using LockManager.LockImplementation;
using PageManager;
using PageManager.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LogManager
{
    public class ReadonlyTransaction : ITransaction
    {
        private ILockManager lockManager;
        private Dictionary<int, LockTypeEnum> locksHeld = new Dictionary<int, LockTypeEnum>();
        private readonly ulong transactionId;
        private object lck = new object();

        public ReadonlyTransaction(ILockManager lockManager, ulong transactionId)
        {
            this.lockManager = lockManager;
            this.transactionId = transactionId;
        }

        public async Task<Releaser> AcquireLock(ulong pageId, LockTypeEnum lockType)
        {
            int lockId = lockManager.LockIdForPage(pageId);

            if (lockType != LockTypeEnum.Exclusive)
            {
                throw new ArgumentException("Can't request EX lock in readonly tran.");
            }

            lock (lck)
            {
                if (locksHeld.ContainsKey(lockId))
                {
                    // Return dummy leaser. You don't really own this lock.
                    // This probably needs to change.
                    throw new TranAlreadyHoldingLock();
                }
            }

            var releaser = await lockManager.AcquireLock(lockType, pageId, 0);

            lock (lck)
            {
                locksHeld.Add(lockId, lockType);
            }

            releaser.SetReleaseCallback(() => this.ReleaseLock(lockId));

            return releaser;
        }

        private void ReleaseLock(int lockId)
        {
            lock (lck)
            {
                this.locksHeld.Remove(lockId);
            }
        }

        public void AddRecord(ILogRecord logRecord)
        {
            throw new InvalidTransactionOperationException();
        }

        public Task Commit()
        {
            throw new InvalidTransactionOperationException();
        }

        public void Dispose()
        {
            if (this.locksHeld.Any())
            {
                throw new TranHoldingLockDuringDispose();
            }
        }

        public ValueTask DisposeAsync()
        {
            this.Dispose();
            return default;
        }

        public IEnumerable<ILogRecord> GetRecords() => Enumerable.Empty<ILogRecord>();

        public TransactionState GetTransactionState() => TransactionState.Open;

        public Task Rollback()
        {
            throw new InvalidTransactionOperationException();
        }

        public ulong TranscationId() => 0;

        public void VerifyLock(ulong pageId, LockTypeEnum expectedLock)
        {
            if (expectedLock == LockTypeEnum.Exclusive)
            {
                throw new TranNotHoldingLock();
            }

            lock (lck)
            {
                if (!this.locksHeld.ContainsKey(this.lockManager.LockIdForPage(pageId)))
                {
                    throw new TranNotHoldingLock();
                }
            }
        }
    }
}
