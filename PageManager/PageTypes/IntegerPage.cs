using System;
using System.IO;

namespace PageManager
{
    public interface IAllocateIntegerPage
    {
        IntegerOnlyPage AllocatePageInt(ulong prevPage, ulong nextPage, ITransaction transaction);
        IntegerOnlyPage GetPageInt(ulong pageId, ITransaction transaction);
    }

    public class IntegerOnlyPage : SimpleTypeOnlyPage<int>
    {
        public IntegerOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId) : base(pageSize, pageId, PageManager.PageType.IntPage, prevPageId, nextPageId) { }
        public IntegerOnlyPage(BinaryReader stream) : base(stream, PageManager.PageType.IntPage) { }

        protected override void SerializeInternal(BinaryReader stream)
        {
            this.items = new int[this.rowCount];

            for (int i = 0; i < this.items.Length; i++)
            {
                this.items[i] = stream.ReadInt32();
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
                int elemDiffPosition = (redoContent.DiffStart - (ushort)IPage.FirstElementPosition) / sizeof(int);
                int elemDiffCount = redoContent.DataToApply.Length / sizeof(int);

                for (int i = 0; i < elemDiffCount; i++)
                {
                    this.items[i + elemDiffPosition] = BitConverter.ToInt32(redoContent.DataToApply, i * sizeof(int));
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
                int elemDiffPosition = (redoContent.DiffStart - (ushort)IPage.FirstElementPosition) / sizeof(int);
                int elemDiffCount = redoContent.DataToUndo.Length / sizeof(int);

                for (int i = 0; i < elemDiffCount; i++)
                {
                    this.items[i + elemDiffPosition] = BitConverter.ToInt32(redoContent.DataToUndo, i * sizeof(int));
                }
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}
