using PageManager;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace LogManager
{
    public class LogManager : ILogManager
    {
        private BinaryWriter storage;
        private object lck = new object();
        private static long lastTransactionId = 0;

        public LogManager(BinaryWriter storage)
        {
            this.storage = storage;
        }

        public async Task Recovery(BinaryReader reader, IPageManager pageManager, ITransaction tran)
        {
            // Phase I.
            // Figure out what is committed.
            bool passCompleted = false;
            HashSet<ulong> committedTransactions = new HashSet<ulong>();
            HashSet<ulong> uncommittedTransactions = new HashSet<ulong>();

            while (!passCompleted)
            {
                if (reader.BaseStream.Position == reader.BaseStream.Length)
                {
                    passCompleted = true;
                    break;
                }

                LogRecordType recordType = (LogRecordType)reader.ReadByte();

                ILogRecord rc = null;
                switch (recordType)
                {
                    case LogRecordType.NullRecord: passCompleted = true; break;
                    case LogRecordType.Commit:
                        ulong transactionId = reader.ReadUInt64();
                        uncommittedTransactions.Remove(transactionId);
                        committedTransactions.Add(transactionId);
                        break;
                    case LogRecordType.Rollback: break;
                    case LogRecordType.RowModify:
                        rc = new UpdateRowRecord(reader);
                        break;
                    case LogRecordType.AllocatePage:
                        rc = new AllocatePageLogRecord(reader);
                        break;
                    case LogRecordType.RowInsert:
                        rc = new InsertRowRecord(reader);
                        break;
                }

                if (rc != null)
                {
                    uncommittedTransactions.Add(rc.TransactionId());
                }
            }

            // Phase II.
            // Redo committed.
            reader.BaseStream.Seek(0, SeekOrigin.Begin);
            List<ILogRecord> undoTranList = new List<ILogRecord>();
            passCompleted = false;
            while (!passCompleted)
            {
                if (reader.BaseStream.Position == reader.BaseStream.Length)
                {
                    passCompleted = true;
                    break;
                }

                LogRecordType recordType = (LogRecordType)reader.ReadByte();

                ILogRecord rc = null;
                switch (recordType)
                {
                    case LogRecordType.NullRecord: passCompleted = true; break;
                    case LogRecordType.Commit: 
                        ulong transactionId = reader.ReadUInt64();
                        break;
                    case LogRecordType.Rollback: break;
                    case LogRecordType.RowModify:
                        rc = new UpdateRowRecord(reader);
                        break;
                    case LogRecordType.RowInsert:
                        rc = new InsertRowRecord(reader);
                        break;
                    case LogRecordType.AllocatePage:
                        rc = new AllocatePageLogRecord(reader);
                        break;
                }

                if (rc != null)
                {
                    if (committedTransactions.Contains(rc.TransactionId()))
                    {
                        await rc.Redo(pageManager, tran);
                    }
                    else
                    {
                        undoTranList.Add(rc);
                    }
                }
            }

            // Phase III.
            // Undo uncommitted.
            undoTranList.Reverse();
            foreach (ILogRecord rc in undoTranList)
            {
                await rc.Undo(pageManager, tran);
            }
        }

        public async Task CommitTransaction(ITransaction tran)
        {
            lock (lck)
            {
                foreach (ILogRecord record in tran.GetRecords())
                {
                    record.Serialize(storage);
                }

                storage.Write((byte)LogRecordType.Commit);
                storage.Write(tran.TranscationId());
            }

            await storage.BaseStream.FlushAsync();
        }

        public async Task Flush()
        {
            await this.storage.BaseStream.FlushAsync();
        }

        public ITransaction CreateTransaction(IPageManager manager, bool isReadOnly, string name)
        {
            long tranId = Interlocked.Increment(ref lastTransactionId);
            if (isReadOnly)
            {
                return new ReadonlyTransaction(manager.GetLockManager(), (ulong)tranId);
            }
            else
            {
                return new Transaction(this, manager, (ulong)tranId, name);
            }
        }

        public ITransaction CreateTransaction(IPageManager manager, string name)
        {
            long tranId = Interlocked.Increment(ref lastTransactionId);
            return new Transaction(this, manager, (ulong)tranId, name);
        }

        public ITransaction CreateTransaction(IPageManager manager)
        {
            long tranId = Interlocked.Increment(ref lastTransactionId);
            return new Transaction(this, manager, (ulong)tranId, "NO_NAME");
        }
    }
}
