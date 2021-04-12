using System.IO;
using System.Threading.Tasks;

namespace PageManager
{
    public enum LogRecordType
    {
        NullRecord,
        RowModify,
        Commit,
        Rollback,
        AllocatePage,
        RowInsert,
        CheckpointStart,
        CheckpointEnd,
    }

    public struct RedoContent
    {
        public readonly byte[] DataToApply;
        public readonly ushort RowPosition;

        public RedoContent(byte[] dataToApply, ushort rowPosition) => (DataToApply, RowPosition) = (dataToApply, rowPosition);
    }

    public struct UndoContent
    {
        public readonly byte[] DataToUndo;
        public readonly ushort RowPosition;

        public UndoContent(byte[] dataToUndo, ushort rowPosition) => (DataToUndo, RowPosition) = (dataToUndo, rowPosition);
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
