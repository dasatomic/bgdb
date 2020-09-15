using PageManager;
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
        public readonly ColumnInfo[] columnInfos;
        public readonly PageType pageType;

        public InsertRowRecord(ulong pageId, ushort rowPosition, byte[] diffNewValue, ulong transactionId, ColumnInfo[] columnInfos, PageType pageType)
        {
            this.PageId = pageId;
            this.RowPosition = rowPosition;
            this.DiffNewValue = diffNewValue;
            this.TranscationId = transactionId;
            this.columnInfos = columnInfos;
            this.pageType = pageType;
        }
        public InsertRowRecord(ulong pageId, ushort rowPosition, byte[] diffNewValue, ulong transactionId, PageType pageType)
        {
            this.PageId = pageId;
            this.RowPosition = rowPosition;
            this.DiffNewValue = diffNewValue;
            this.TranscationId = transactionId;
            this.columnInfos = new ColumnInfo[0];
            this.pageType = pageType;
        }

        public InsertRowRecord(BinaryReader source)
        {
            this.TranscationId = source.ReadUInt64();
            this.PageId = source.ReadUInt64();
            this.RowPosition = source.ReadUInt16();
            int bc = source.ReadUInt16();
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
            destination.Write((byte)LogRecordType.RowInsert);
            destination.Write(this.TranscationId);
            destination.Write(this.PageId);
            destination.Write(this.RowPosition);
            destination.Write((ushort)this.DiffNewValue.Length);
            destination.Write(this.DiffNewValue);
            destination.Write((ushort)this.columnInfos.Length);
            foreach (ColumnInfo ct in this.columnInfos)
            {
                ct.Serialize(destination);
            }
            destination.Write((byte)this.pageType);
        }

        public LogRecordType GetRecordType() => LogRecordType.RowInsert;

        public ulong TransactionId() => this.TranscationId;

        public async Task Redo(IPageManager pageManager, ITransaction tran)
        {
            IPage page = await pageManager.GetPage(this.PageId, tran, this.pageType, this.columnInfos).ConfigureAwait(false);
            page.RedoLog(this, tran);
        }

        public async Task Undo(IPageManager pageManager, ITransaction tran)
        {
            tran.VerifyLock(this.PageId, LockManager.LockTypeEnum.Exclusive);
            IPage page = await pageManager.GetPage(this.PageId, tran, this.pageType, this.columnInfos).ConfigureAwait(false);
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
