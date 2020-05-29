using LogManager;
using System;
using System.IO;
using System.Linq;

namespace PageManager
{
    public interface IAllocateIntegerPage
    {
        IntegerOnlyPage AllocatePageInt(ulong prevPage, ulong nextPage, ITransaction transaction);
        IntegerOnlyPage GetPageInt(ulong pageId, ITransaction transaction);
    }

    public class IntegerOnlyPage : SimpleTypeOnlyPage<int>
    {
        public IntegerOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId, ITransaction transaction) 
            : base(pageSize, pageId, PageManager.PageType.IntPage, prevPageId, nextPageId, transaction) { }
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
            var redoContent = record.GetRedoContent();
            if (record.GetRecordType() == LogRecordType.RowModify)
            {
                this.items[redoContent.RowPosition] = BitConverter.ToInt32(redoContent.DataToApply);
            }
            else if (record.GetRecordType() == LogRecordType.RowInsert)
            {
                if (redoContent.RowPosition != items.Length)
                {
                    throw new LogCorruptedException();
                }

                // TODO: Perf is terrible.
                // Maybe list is better choice for values?
                int val = BitConverter.ToInt32(redoContent.DataToApply);
                this.items = this.items.Concat(new int[1] { val }).ToArray();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public override void UndoLog(ILogRecord record, ITransaction tran)
        {
            var undoContent = record.GetUndoContent();
            if (record.GetRecordType() == LogRecordType.RowModify)
            {
                this.items[undoContent.RowPosition] = BitConverter.ToInt32(undoContent.DataToUndo);
            }
            else if (record.GetRecordType() == LogRecordType.RowInsert)
            {
                if (this.items.Length != undoContent.RowPosition + 1)
                {
                    throw new LogCorruptedException();
                }

                this.items = this.items.Take(this.items.Length - 1).ToArray();
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}
