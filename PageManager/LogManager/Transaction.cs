using LockManager;
using LockManager.LockImplementation;
using PageManager;
using PageManager.Exceptions;
using PageManager.LogManager;
using System;
using System.Collections.Generic;
using System.IO;
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
        private List<Releaser> myLocks = new List<Releaser>();
        private IsolationLevelEnum isolationLevel;
        private object lck = new object();
        private Queue<DirectoryInfo> tempDirectoriesToCleanUp = new Queue<DirectoryInfo>();

        public Transaction(ILogManager logManager, IPageManager pageManager, ulong transactionId, string name)
            : this(logManager, pageManager, transactionId, name, IsolationLevelEnum.ReadCommitted) { }

        public Transaction(ILogManager logManager, IPageManager pageManager, ulong transactionId, string name, IsolationLevelEnum isolationLevel)
        {
            this.transactionId = transactionId;
            this.logRecords = new List<ILogRecord>();
            this.logManager = logManager;
            this.name = name;
            this.state = TransactionState.Open;
            this.pageManager = pageManager;
            this.isolationLevel = isolationLevel;

        }

        public void AddRecord(ILogRecord logRecord)
        {
            this.logRecords.Add(logRecord);
        }

        public async Task Commit()
        {
            if (this.state != TransactionState.Open)
            {
                throw new InvalidTransactionOperationException();
            }

            await this.logManager.CommitTransaction(this).ConfigureAwait(false);
            this.state = TransactionState.Committed;

            this.myLocks.Reverse();
            foreach (Releaser releaser in this.myLocks)
            {
                releaser.Dispose();
            }

            this.myLocks.Clear();
        }

        public async Task Rollback()
        {
            if (this.state != TransactionState.Open)
            {
                throw new InvalidTransactionOperationException();
            }

            this.logRecords.Reverse();
            foreach (ILogRecord record in this.logRecords)
            {
                await record.Undo(this.pageManager, this).ConfigureAwait(false);
            }

            this.state = TransactionState.RollBacked;

            this.myLocks.Reverse();
            foreach (Releaser releaser in this.myLocks)
            {
                releaser.Dispose();
            }

            this.myLocks.Clear();
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

            if (this.myLocks.Any())
            {
                throw new TranHoldingLockDuringDispose();
            }

            foreach (DirectoryInfo dir in this.tempDirectoriesToCleanUp)
            {
                Directory.Delete(dir.FullName, true);
            }

            this.pageManager.GetLockManager().ReleaseOwner(this.transactionId);
        }

        public async ValueTask DisposeAsync()
        {
            if (this.state == TransactionState.Open)
            {
                await this.Rollback().ConfigureAwait(false);
            }

            if (this.locksHeld.Any())
            {
                throw new TranHoldingLockDuringDispose();
            }

            if (this.myLocks.Any())
            {
                throw new TranHoldingLockDuringDispose();
            }

            while (this.tempDirectoriesToCleanUp.Any())
            {
                DirectoryInfo dir = this.tempDirectoriesToCleanUp.Dequeue();
                Directory.Delete(dir.FullName, true);
            }

            this.pageManager.GetLockManager().ReleaseOwner(this.transactionId);
        }

        private async Task<Releaser> AcquireLockInternal(ulong pageId, LockTypeEnum lockType, bool forceCallerOwnership)
        {
            ILockManager lockManager = this.pageManager.GetLockManager();
            int lockId = lockManager.LockIdForPage(pageId);

            lock (lck)
            {
                if (locksHeld.ContainsKey(lockId))
                {
                    return new Releaser();
                }
            }

            var releaser = await lockManager.AcquireLock(lockType, pageId, this.transactionId).ConfigureAwait(continueOnCapturedContext: false);

            lock (lck)
            {
                locksHeld.Add(lockId, lockType);
            }

            releaser.SetReleaseCallback(() => this.ReleaseLockCallback(lockId));

            if (forceCallerOwnership)
            {
                return releaser;
            }

            // TODO: Implement Isolation Level strategy.
            if (this.isolationLevel == IsolationLevelEnum.ReadCommitted)
            {
                // If this is a read lock return to caller.
                // If write transaction is the owner.
                if (lockType == LockTypeEnum.Shared)
                {
                    return releaser;
                }
                else if (lockType == LockTypeEnum.Exclusive)
                {
                    this.myLocks.Add(releaser);
                    return new Releaser();
                }
                else
                {
                    throw new InvalidProgramException();
                }
            }
            else
            {
                throw new InvalidProgramException();
            }
        }

        public async Task<Releaser> AcquireLockWithCallerOwnership(ulong pageId, LockTypeEnum lockType)
        {
            return await AcquireLockInternal(pageId, lockType, true).ConfigureAwait(false);
        }

        public async Task<Releaser> AcquireLock(ulong pageId, LockTypeEnum lockType)
        {
            return await AcquireLockInternal(pageId, lockType, false).ConfigureAwait(false);
        }

        private void ReleaseLockCallback(int lockId)
        {
            lock (lck)
            {
                this.locksHeld.Remove(lockId);
            }
        }

        public bool AmIHoldingALock(ulong pageId, out LockTypeEnum lockType)
        {
            ILockManager lockManager = this.pageManager.GetLockManager();
            int lockId = lockManager.LockIdForPage(pageId);

            lock (lck)
            {
                if (locksHeld.TryGetValue(lockId, out LockTypeEnum myLock))
                {
                    lockType = myLock;
                    return true;
                }
            }

            lockType = LockTypeEnum.Shared;
            return false;
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

        public IEnumerable<ulong> LockedPages()
        {
            throw new NotImplementedException();
        }

        public void RegisterTempFolder(DirectoryInfo tempFolder)
        {
            this.tempDirectoriesToCleanUp.Enqueue(tempFolder);
        }
    }
}
