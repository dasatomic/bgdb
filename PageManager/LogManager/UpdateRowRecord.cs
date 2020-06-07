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
        public readonly ColumnType[] columnTypes;
        public readonly PageType pageType;

        public UpdateRowRecord(ulong pageId, ushort rowPosition, byte[] diffOldValue, byte[] diffNewValue, ulong transactionId, ColumnType[] columnTypes, PageType pageType)
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
            this.columnTypes = columnTypes;
            this.pageType = pageType;
        }

        public UpdateRowRecord(ulong pageId, ushort rowPosition, byte[] diffOldValue, byte[] diffNewValue, ulong transactionId, PageType pageType)
            : this(pageId, rowPosition, diffOldValue, diffNewValue, transactionId, new ColumnType[0], pageType)
        { }

        public UpdateRowRecord(BinaryReader source)
        {
            this.TranscationId = source.ReadUInt64();
            this.PageId = source.ReadUInt64();
            this.RowPosition = source.ReadUInt16();
            int bc = source.ReadUInt16();
            this.DiffOldValue = source.ReadBytes(bc);
            this.DiffNewValue = source.ReadBytes(bc);
            int cts = source.ReadUInt16();
            this.columnTypes = new ColumnType[cts];
            for (int i = 0; i < cts; i++)
            {
                this.columnTypes[i] = (ColumnType)source.ReadByte();
            }

            this.pageType = (PageType)source.ReadByte();
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
            destination.Write((ushort)this.columnTypes.Length);
            foreach (ColumnType ct in this.columnTypes)
            {
                destination.Write((byte)ct);
            }
            destination.Write((byte)this.pageType);
        }

        public LogRecordType GetRecordType() => LogRecordType.RowModify;

        public ulong TransactionId() => this.TranscationId;

        public async Task Redo(IPageManager pageManager, ITransaction tran)
        {
            IPage page = pageManager.GetPage(this.PageId, tran, this.pageType, this.columnTypes);
            page.RedoLog(this, tran);
        }

        public async Task Undo(IPageManager pageManager, ITransaction tran)
        {
            IPage page = pageManager.GetPage(this.PageId, tran, this.pageType, this.columnTypes);
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
