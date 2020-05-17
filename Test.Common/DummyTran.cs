using PageManager;
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

        public IEnumerable<ILogRecord> GetRecords()
        {
            return Enumerable.Empty<ILogRecord>();
        }

        public void Rollback()
        {
        }

        public ulong TranscationId()
        {
            return 42;
        }
    }
}
