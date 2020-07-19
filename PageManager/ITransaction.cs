using LockManager;
using LockManager.LockImplementation;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PageManager
{
    public enum TransactionState
    {
        Open,
        Committed,
        RollBacked,
    }

    public interface ITransaction : IAsyncDisposable, IDisposable
    {
        ulong TranscationId();
        Task Rollback();
        Task Commit();
        void AddRecord(ILogRecord logRecord);
        IEnumerable<ILogRecord> GetRecords();
        TransactionState GetTransactionState();
        Task<Releaser> AcquireLock(ulong pageId, LockTypeEnum lockType);
        Task<Releaser> AcquireLockWithCallerOwnership(ulong pageId, LockTypeEnum lockType);
        void VerifyLock(ulong pageId, LockTypeEnum expectedLock);
        public bool AmIHoldingALock(ulong pageId, out LockTypeEnum lockType);
    }
}
