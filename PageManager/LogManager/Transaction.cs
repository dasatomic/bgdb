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
    public class Transaction : ITransaction
    {
        private readonly ulong transactionId;
        private List<ILogRecord> logRecords;
        private IPageManager pageManager;
        private readonly ILogManager logManager;
        private readonly string name;
        private TransactionState state;
        private Dictionary<int, LockTypeEnum> locksHeld = new Dictionary<int, LockTypeEnum>();
        private object lck = new object();

        public Transaction(ILogManager logManager, IPageManager pageManager, ulong transactionId, string name)
        {
            this.transactionId = transactionId;
            this.logRecords = new List<ILogRecord>();
            this.logManager = logManager;
            this.name = name;
            this.state = TransactionState.Open;
            this.pageManager = pageManager;
        }

        public void AddRecord(ILogRecord logRecord)
        {
            this.logRecords.Add(logRecord);
        }

        public async Task Commit()
        {
            await this.logManager.CommitTransaction(this);
            this.state = TransactionState.Committed;
        }

        public async Task Rollback()
        {
            this.logRecords.Reverse();
            foreach (ILogRecord record in this.logRecords)
            {
                await record.Undo(this.pageManager, this);
            }

            this.state = TransactionState.RollBacked;
        }

        public ulong TranscationId() => this.transactionId;

        public IEnumerable<ILogRecord> GetRecords()
        {
            return this.logRecords.AsEnumerable();
        }

        public TransactionState GetTransactionState() => this.state;

        public void Dispose()
        {
            if (this.state == TransactionState.Open)
            {
                this.Rollback().Wait();
            }

            if (this.locksHeld.Any())
            {
                throw new TranHoldingLockDuringDispose();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (this.state == TransactionState.Open)
            {
                await this.Rollback();
            }

            if (this.locksHeld.Any())
            {
                throw new TranHoldingLockDuringDispose();
            }
        }

        public async Task<Releaser> AcquireLock(ulong pageId, LockTypeEnum lockType)
        {
            ILockManager lockManager = this.pageManager.GetLockManager();
            int lockId = lockManager.LockIdForPage(pageId);

            lock (lck)
            {
                if (locksHeld.ContainsKey(lockId))
                {
                    // Return dummy leaser. You don't really own this lock.
                    // This probably needs to change.
                    return new Releaser();
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

        public void VerifyLock(ulong pageId, LockTypeEnum expectedLock)
        {
            int lockId = this.pageManager.GetLockManager().LockIdForPage(pageId);

            lock (lck)
            {
                if (this.locksHeld.TryGetValue(lockId, out LockTypeEnum lockHeld))
                {
                    if ((int)lockHeld < (int)expectedLock)
                    {
                        throw new TranNotHoldingLock();
                    }
                }
                else
                {
                    throw new TranNotHoldingLock();
                }
            }
        }
    }
}
