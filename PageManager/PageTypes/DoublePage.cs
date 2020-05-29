using LogManager;
using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;

namespace PageManager
{
    public interface IAllocateDoublePage
    {
        DoubleOnlyPage AllocatePageDouble(ulong prevPage, ulong nextPage, ITransaction tran);
        DoubleOnlyPage GetPageDouble(ulong pageId, ITransaction tran);
    }

    public class DoubleOnlyPage : SimpleTypeOnlyPage<double>
    {
        public DoubleOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId, ITransaction tran) : 
            base(pageSize, pageId, PageManager.PageType.DoublePage, prevPageId, nextPageId, tran) { }
        public DoubleOnlyPage(BinaryReader stream) : base(stream, PageManager.PageType.DoublePage) { }

        protected override void SerializeInternal(BinaryReader stream)
        {
            this.items = new double[this.rowCount];

            for (int i = 0; i < this.items.Length; i++)
            {
                this.items[i] = stream.ReadDouble();
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
                this.items[redoContent.RowPosition] = BitConverter.ToDouble(redoContent.DataToApply);
            }
            else if (record.GetRecordType() == LogRecordType.RowInsert)
            {
                if (redoContent.RowPosition != items.Length)
                {
                    throw new LogCorruptedException();
                }

                // TODO: Perf is terrible.
                // Maybe list is better choice for values?
                double val = BitConverter.ToDouble(redoContent.DataToApply);
                this.items = this.items.Concat(new double[1] { val }).ToArray();
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
                this.items[undoContent.RowPosition] = BitConverter.ToDouble(undoContent.DataToUndo);
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
