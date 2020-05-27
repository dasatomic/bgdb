using PageManager;
using System;
using System.IO;
using System.Threading.Tasks;

namespace LogManager
{

    public struct PageModifyRecord : ILogRecord
    {
        public readonly ulong PageId;
        public readonly ushort PageOffsetDiffStart;
        public readonly byte[] DiffOldValue;
        public readonly byte[] DiffNewValue;
        public readonly ulong TranscationId;

        public PageModifyRecord(ulong pageId, ushort pageOffsetDiffStart, byte[] diffOldValue, byte[] diffNewValue, ulong transactionId)
        {
            if (diffOldValue.Length != diffNewValue.Length)
            {
                throw new ArgumentException();
            }

            this.PageId = pageId;
            this.PageOffsetDiffStart = pageOffsetDiffStart;
            this.DiffOldValue = diffOldValue;
            this.DiffNewValue = diffNewValue;
            this.TranscationId = transactionId;
        }

        public PageModifyRecord(BinaryReader source)
        {
            this.TranscationId = source.ReadUInt64();
            this.PageId = source.ReadUInt64();
            this.PageOffsetDiffStart = source.ReadUInt16();
            int bc = source.ReadUInt16();
            this.DiffOldValue = source.ReadBytes(bc);
            this.DiffNewValue = source.ReadBytes(bc);
        }

        public void Serialize(BinaryWriter destination)
        {
            destination.Write((byte)LogRecordType.PageModify);
            destination.Write(this.TranscationId);
            destination.Write(this.PageId);
            destination.Write(this.PageOffsetDiffStart);
            destination.Write((ushort)this.DiffOldValue.Length);
            destination.Write(this.DiffOldValue);
            destination.Write(this.DiffNewValue);
        }

        public LogRecordType GetRecordType() => LogRecordType.PageModify;

        public ulong TransactionId() => this.TranscationId;

        public async Task Redo(IPageManager pageManager, ITransaction tran)
        {
            // TODO log interaction should be page type agnostic.
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
            return new RedoContent(this.DiffNewValue, this.PageOffsetDiffStart);
        }

        public UndoContent GetUndoContent()
        {
            return new UndoContent(this.DiffOldValue, this.PageOffsetDiffStart);
        }
    }
}
