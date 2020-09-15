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

        // TODO: Update Row shouldn't care about Column types?
        public readonly ColumnInfo[] columnInfos;
        public readonly PageType pageType;

        public UpdateRowRecord(ulong pageId, ushort rowPosition, byte[] diffOldValue, byte[] diffNewValue, ulong transactionId, ColumnInfo[] columnInfos, PageType pageType)
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
            this.columnInfos = columnInfos;
            this.pageType = pageType;
        }

        public UpdateRowRecord(ulong pageId, ushort rowPosition, byte[] diffOldValue, byte[] diffNewValue, ulong transactionId, PageType pageType)
            : this(pageId, rowPosition, diffOldValue, diffNewValue, transactionId, new ColumnInfo[0], pageType)
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
            this.columnInfos = new ColumnInfo[cts];
            for (int i = 0; i < cts; i++)
            {
                this.columnInfos[i] = ColumnInfo.Deserialize(source);
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
            destination.Write((ushort)this.columnInfos.Length);
            foreach (ColumnInfo ci in this.columnInfos)
            {
                ci.Serialize(destination);
            }
            destination.Write((byte)this.pageType);
        }

        public LogRecordType GetRecordType() => LogRecordType.RowModify;

        public ulong TransactionId() => this.TranscationId;

        public async Task Redo(IPageManager pageManager, ITransaction tran)
        {
            IPage page = await pageManager.GetPage(this.PageId, tran, this.pageType, this.columnInfos);
            page.RedoLog(this, tran);
        }

        public async Task Undo(IPageManager pageManager, ITransaction tran)
        {
            IPage page = await pageManager.GetPage(this.PageId, tran, this.pageType, this.columnInfos);
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
