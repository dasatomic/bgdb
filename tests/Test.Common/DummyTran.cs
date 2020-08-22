using LockManager;
using LockManager.LockImplementation;
using PageManager;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Test.Common
{
    public class DummyTran : ITransaction
    {
        public async Task<Releaser> AcquireLock(ulong pageId, LockTypeEnum lockType)
        {
            return await Task.FromResult(new Releaser());
        }

        public async Task<Releaser> AcquireLockWithCallerOwnership(ulong pageId, LockTypeEnum lockType)
        {
            return await Task.FromResult(new Releaser());
        }

        public void AddRecord(ILogRecord logRecord)
        {
        }

        public bool AmIHoldingALock(ulong pageId, out LockTypeEnum lockType)
        {
            lockType = LockTypeEnum.Exclusive;
            return false;
        }

        public Task Commit()
        {
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => default;

        public IEnumerable<ILogRecord> GetRecords()
        {
            return Enumerable.Empty<ILogRecord>();
        }

        public TransactionState GetTransactionState()
        {
            return TransactionState.Committed;
        }

        public Task Rollback()
        {
            return Task.CompletedTask;
        }

        public ulong TranscationId()
        {
            return 42;
        }

        public void VerifyLock(ulong pageId, LockTypeEnum expectedLock)
        {
        }
    }
}
