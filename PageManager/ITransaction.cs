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

    public interface ITransaction : IDisposable
    {
        ulong TranscationId();
        Task Rollback();
        Task Commit();
        void AddRecord(ILogRecord logRecord);
        IEnumerable<ILogRecord> GetRecords();
        TransactionState GetTransactionState();
    }
}
