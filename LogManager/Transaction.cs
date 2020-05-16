using PageManager;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LogManager
{
    public class Transaction : ITransaction
    {
        public static ulong lastTransactionId = 0;

        private readonly ulong transactionId;
        private List<ILogRecord> logRecords;
        private readonly ILogManager logManager;

        public Transaction(ILogManager logManager)
        {
            transactionId = lastTransactionId++;
            logRecords = new List<ILogRecord>();
            this.logManager = logManager;
        }

        public void AddRecord(ILogRecord logRecord)
        {
            this.logRecords.Add(logRecord);
        }

        public async Task Commit()
        {
            await this.logManager.CommitTransaction(this);
        }

        public void Rollback()
        {
            // No op while content is in memory.
            // once we start pushing log to disk before commit this needs to change.
        }

        public ulong TranscationId() => this.transactionId;

        public IEnumerable<ILogRecord> GetRecords()
        {
            return this.logRecords.AsEnumerable();
        }
    }
}
