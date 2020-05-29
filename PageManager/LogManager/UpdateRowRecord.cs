using PageManager;
using System;
using System.IO;
using System.Threading.Tasks;

namespace LogManager
{
    public struct UpdateRowRecord : ILogRecord
    {
        public readonly ulong PageId;
        public readonly ushort RowPosition;
        public readonly byte[] DiffOldValue;
        public readonly byte[] DiffNewValue;
        public readonly ulong TranscationId;

        public UpdateRowRecord(ulong pageId, ushort rowPosition, byte[] diffOldValue, byte[] diffNewValue, ulong transactionId)
        {
            if (diffOldValue.Length != diffNewValue.Length)
            {
                throw new ArgumentException();
            }

            this.PageId = pageId;
            this.RowPosition = rowPosition;
            this.DiffOldValue = diffOldValue;
            this.DiffNewValue = diffNewValue;
            this.TranscationId = transactionId;
        }

        public UpdateRowRecord(BinaryReader source)
        {
            this.TranscationId = source.ReadUInt64();
            this.PageId = source.ReadUInt64();
            this.RowPosition = source.ReadUInt16();
            int bc = source.ReadUInt16();
            this.DiffOldValue = source.ReadBytes(bc);
            this.DiffNewValue = source.ReadBytes(bc);
        }

        public void Serialize(BinaryWriter destination)
        {
            destination.Write((byte)LogRecordType.RowModify);
            destination.Write(this.TranscationId);
            destination.Write(this.PageId);
            destination.Write(this.RowPosition);
            destination.Write((ushort)this.DiffOldValue.Length);
            destination.Write(this.DiffOldValue);
            destination.Write(this.DiffNewValue);
        }

        public LogRecordType GetRecordType() => LogRecordType.RowModify;

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
            return new UndoContent(this.DiffOldValue, this.RowPosition);
        }
    }
}
