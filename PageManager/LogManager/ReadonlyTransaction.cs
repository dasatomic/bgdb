using LockManager;
using LockManager.LockImplementation;
using PageManager;
using PageManager.Exceptions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
        private Queue<DirectoryInfo> tempDirectoriesToCleanUp = new Queue<DirectoryInfo>();

        public ReadonlyTransaction(ILockManager lockManager, ulong transactionId)
        {
            this.lockManager = lockManager;
            this.transactionId = transactionId;
        }

        public async Task<Releaser> AcquireLockWithCallerOwnership(ulong pageId, LockTypeEnum lockType)
        {
            return await AcquireLock(pageId, lockType);
        }

        public async Task<Releaser> AcquireLock(ulong pageId, LockTypeEnum lockType)
        {
            int lockId = lockManager.LockIdForPage(pageId);

            if (lockType != LockTypeEnum.Shared)
            {
                throw new ReadOnlyTranCantAcquireExLockException();
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

            var releaser = await lockManager.AcquireLock(lockType, pageId, this.transactionId);

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

            this.lockManager.ReleaseOwner(this.transactionId);

            while (this.tempDirectoriesToCleanUp.Any())
            {
                DirectoryInfo dir = this.tempDirectoriesToCleanUp.Dequeue();
                Directory.Delete(dir.FullName, true);
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

        public ulong TranscationId() => this.transactionId;

        public bool VerifyLock(ulong pageId, LockTypeEnum expectedLock)
        {
            if (expectedLock == LockTypeEnum.Exclusive)
            {
                Debug.Assert(false);
                return false;
            }

            lock (lck)
            {
                if (!this.locksHeld.ContainsKey(this.lockManager.LockIdForPage(pageId)))
                {
                    Debug.Assert(false);
                    return false;
                }
            }

            return true;
        }

        public bool AmIHoldingALock(ulong pageId, out LockTypeEnum lockType)
        {
            lock (lck)
            {
                lockType = LockTypeEnum.Shared;
                return this.locksHeld.ContainsKey(this.lockManager.LockIdForPage(pageId));
            }
        }

        public void RegisterTempFolder(DirectoryInfo tempFolder)
        {
            this.tempDirectoriesToCleanUp.Enqueue(tempFolder);
        }
    }
}
