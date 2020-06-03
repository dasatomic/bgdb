using PageManager;
using PageManager.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LogManager
{
    public class ReadonlyTransaction : ITransaction
    {
        private readonly ulong transactionId;

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
            // TODO: Release locks
        }

        public IEnumerable<ILogRecord> GetRecords() => Enumerable.Empty<ILogRecord>();

        public TransactionState GetTransactionState() => TransactionState.Open;

        public Task Rollback()
        {
            throw new InvalidTransactionOperationException();
        }

        public ulong TranscationId() => this.transactionId;
    }
}
