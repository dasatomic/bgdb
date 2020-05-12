using System.IO;

namespace LogManager
{
    public class LogManager : ILogManager
    {
        private BinaryWriter storage;

        public LogManager(BinaryWriter storage)
        {
            this.storage = storage;
        }

        public void CommitTransaction(ITransaction tran)
        {
            foreach (ILogRecord record in tran.GetRecords())
            {
                record.Serialize(storage);
            }

            storage.Write((byte)LogRecordType.Commit);
            storage.Write(tran.TranscationId());

            storage.Flush();
        }
    }
}
