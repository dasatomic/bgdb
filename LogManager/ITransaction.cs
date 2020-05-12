using System.Collections;
using System.Collections.Generic;

namespace LogManager
{
    public interface ITransaction 
    {
        ulong TranscationId();
        void Rollback();
        void Commit();
        void AddRecord(ILogRecord logRecord);
        IEnumerable<ILogRecord> GetRecords();
    }
}
