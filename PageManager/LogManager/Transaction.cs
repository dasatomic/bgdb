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
        private IPageManager pageManager;
        private readonly ILogManager logManager;
        private readonly string name;
        private TransactionState state;

        public Transaction(ILogManager logManager, IPageManager pageManager, string name)
        {
            transactionId = lastTransactionId++;
            logRecords = new List<ILogRecord>();
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
        }

        public async ValueTask DisposeAsync()
        {
            if (this.state == TransactionState.Open)
            {
                await this.Rollback();
            }
        }
    }
}
