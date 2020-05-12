using System.Collections.Generic;
using System.Threading.Tasks;

namespace LogManager
{
    public interface ITransaction 
    {
        ulong TranscationId();
        void Rollback();
        Task Commit();
        void AddRecord(ILogRecord logRecord);
        IEnumerable<ILogRecord> GetRecords();
    }
}
