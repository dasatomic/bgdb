using System.IO;

namespace PageManager
{
    public enum LogRecordType
    {
        PageModify,
        Commit,
        Rollback,
    }

    public interface ILogRecord
    {
        void Serialize(BinaryWriter destination);
        LogRecordType GetRecordType();
        ulong TransactionId();
        void Redo(IPageManager pageManager);
        void Undo(IPageManager pageManager);
    }
}
