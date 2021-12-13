using LockManager;
using LockManager.LockImplementation;
using PageManager;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace LogManager
{
    public class NotLoggedTransaction : ITransaction
    {
        public async Task<Releaser> AcquireLock(ulong pageId, LockTypeEnum lockType)
        {
            // TODO: Even not logged tran needs to go through locks..
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
            lockType = LockTypeEnum.Shared;
            return true;
        }

        public Task Commit()
        {
            throw new InvalidOperationException("Not logged tran can't be committed");
        }

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => default;

        public IEnumerable<ILogRecord> GetRecords() => Enumerable.Empty<ILogRecord>();

        public TransactionState GetTransactionState() => TransactionState.Open;

        public void RegisterTempFolder(DirectoryInfo tempFolder)
        {
            throw new NotImplementedException("Not logged tran shouldn't mess with file system");
        }

        public Task Rollback()
        {
            throw new InvalidOperationException("Not logged tran can't be rolled back");
        }

        public ulong TranscationId() => 0;

        public bool VerifyLock(ulong pageId, LockTypeEnum expectedLock) => true;
    }
}
