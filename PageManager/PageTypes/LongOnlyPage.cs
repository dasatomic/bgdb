using System;
using System.IO;

namespace PageManager
{
    public interface IAllocateLongPage
    {
        LongOnlyPage AllocatePageLong(ulong prevPage, ulong nextPage, ITransaction tran);
        LongOnlyPage GetPageLong(ulong pageId, ITransaction tran);
    }

    public class LongOnlyPage : SimpleTypeOnlyPage<long>
    {
        public LongOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId) : base(pageSize, pageId, PageManager.PageType.LongPage, prevPageId, nextPageId) { }
        public LongOnlyPage(BinaryReader stream) : base(stream, PageManager.PageType.LongPage) { }

        protected override void SerializeInternal(BinaryReader stream)
        {
            this.items = new long[this.rowCount];

            for (int i = 0; i < this.items.Length; i++)
            {
                this.items[i] = stream.ReadInt64();
            }
        }

        public override void Persist(Stream destination)
        {
            using (BinaryWriter bw = new BinaryWriter(destination))
            {
                bw.Write(this.pageId);
                bw.Write(this.pageSize);
                bw.Write((int)this.PageType());
                bw.Write(this.rowCount);
                bw.Write(this.prevPageId);
                bw.Write(this.nextPageId);

                for (int i = 0; i < this.rowCount; i++)
                {
                    bw.Write(this.items[i]);
                }
            }
        }

        public override void RedoLog(ILogRecord record, ITransaction tran)
        {
            if (record.GetRecordType() == LogRecordType.PageModify)
            {
                var redoContent = record.GetRedoContent();
                int elemDiffPosition = (redoContent.DiffStart - (ushort)IPage.FirstElementPosition) / sizeof(long);
                int elemDiffCount = redoContent.DataToApply.Length / sizeof(long);

                for (int i = 0; i < elemDiffCount; i++)
                {
                    this.items[i + elemDiffPosition] = BitConverter.ToInt64(redoContent.DataToApply, i * sizeof(long));
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public override void UndoLog(ILogRecord record, ITransaction tran)
        {
            if (record.GetRecordType() == LogRecordType.PageModify)
            {
                var redoContent = record.GetUndoContent();
                int elemDiffPosition = (redoContent.DiffStart - (ushort)IPage.FirstElementPosition) / sizeof(long);
                int elemDiffCount = redoContent.DataToUndo.Length / sizeof(long);

                for (int i = 0; i < elemDiffCount; i++)
                {
                    this.items[i + elemDiffPosition] = BitConverter.ToInt64(redoContent.DataToUndo, i * sizeof(long));
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}
