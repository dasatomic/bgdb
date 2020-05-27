using System.IO;
using System.Threading.Tasks;

namespace PageManager
{
    public enum LogRecordType
    {
        PageModify,
        Commit,
        Rollback,
    }

    public struct RedoContent
    {
        public readonly byte[] DataToApply;
        public readonly ushort DiffStart;

        public RedoContent(byte[] dataToApply, ushort diffStart) => (DataToApply, DiffStart) = (dataToApply, diffStart);
    }

    public struct UndoContent
    {
        public readonly byte[] DataToUndo;
        public readonly ushort DiffStart;

        public UndoContent(byte[] dataToUndo, ushort diffStart) => (DataToUndo, DiffStart) = (dataToUndo, diffStart);
    }

    public interface ILogRecord
    {
        void Serialize(BinaryWriter destination);
        LogRecordType GetRecordType();
        ulong TransactionId();
        Task Redo(IPageManager pageManager, ITransaction tran);
        Task Undo(IPageManager pageManager, ITransaction tran);
        RedoContent GetRedoContent();
        UndoContent GetUndoContent();
    }
}
