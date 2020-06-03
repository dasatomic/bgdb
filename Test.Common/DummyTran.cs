﻿using PageManager;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Test.Common
{
    public class DummyTran : ITransaction
    {
        public void AddRecord(ILogRecord logRecord)
        {
        }

        public Task Commit()
        {
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }

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
    }
}