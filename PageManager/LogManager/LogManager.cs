using PageManager;
using System.IO;
using System.Threading.Tasks;

namespace LogManager
{
    public class LogManager : ILogManager
    {
        private BinaryWriter storage;

        public LogManager(BinaryWriter storage)
        {
            this.storage = storage;
        }

        public async Task CommitTransaction(ITransaction tran)
        {
            foreach (ILogRecord record in tran.GetRecords())
            {
                record.Serialize(storage);
            }

            storage.Write((byte)LogRecordType.Commit);
            storage.Write(tran.TranscationId());

            await storage.BaseStream.FlushAsync();
        }

        public async Task Flush()
        {
            await this.storage.BaseStream.FlushAsync();
        }
    }
}
