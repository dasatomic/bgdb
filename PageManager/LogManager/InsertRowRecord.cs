using PageManager;
using System;
using System.IO;
using System.Threading.Tasks;

namespace LogManager
{
    public struct InsertRowRecord : ILogRecord
    {
        public readonly ulong PageId;
        public readonly ushort RowPosition;
        public readonly byte[] DiffNewValue;
        public readonly ulong TranscationId;

        public InsertRowRecord(ulong pageId, ushort rowPosition, byte[] diffNewValue, ulong transactionId)
        {
            this.PageId = pageId;
            this.RowPosition = rowPosition;
            this.DiffNewValue = diffNewValue;
            this.TranscationId = transactionId;
        }

        public InsertRowRecord(BinaryReader source)
        {
            this.TranscationId = source.ReadUInt64();
            this.PageId = source.ReadUInt64();
            this.RowPosition = source.ReadUInt16();
            int bc = source.ReadUInt16();
            this.DiffNewValue = source.ReadBytes(bc);
        }

        public void Serialize(BinaryWriter destination)
        {
            destination.Write((byte)LogRecordType.RowModify);
            destination.Write(this.TranscationId);
            destination.Write(this.PageId);
            destination.Write(this.RowPosition);
            destination.Write((ushort)this.DiffNewValue.Length);
            destination.Write(this.DiffNewValue);
        }

        public LogRecordType GetRecordType() => LogRecordType.RowInsert;

        public ulong TransactionId() => this.TranscationId;

        public async Task Redo(IPageManager pageManager, ITransaction tran)
        {
            IPage page = pageManager.GetPage(this.PageId, tran);
            page.RedoLog(this, tran);
        }

        public async Task Undo(IPageManager pageManager, ITransaction tran)
        {
            IPage page = pageManager.GetPage(this.PageId, tran);
            page.UndoLog(this, tran);
        }

        public RedoContent GetRedoContent()
        {
            return new RedoContent(this.DiffNewValue, this.RowPosition);
        }

        public UndoContent GetUndoContent()
        {
            return new UndoContent(null, this.RowPosition);
        }
    }
}
