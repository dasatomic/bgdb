using LogManager;
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace PageManager
{
    public interface IAllocateLongPage
    {
        Task<LongOnlyPage> AllocatePageLong(ulong prevPage, ulong nextPage, ITransaction tran);
        Task<LongOnlyPage> GetPageLong(ulong pageId, ITransaction tran);
    }

    public class LongOnlyPage : SimpleTypeOnlyPage<long>
    {
        public LongOnlyPage(uint pageSize, ulong pageId, ulong prevPageId, ulong nextPageId, ITransaction tran) : 
            base(pageSize, pageId, global::PageManager.PageType.LongPage, prevPageId, nextPageId, tran) { }
        public LongOnlyPage(BinaryReader stream) : base(stream, global::PageManager.PageType.LongPage) { }

        protected override void SerializeInternal(BinaryReader stream)
        {
            this.items = new long[this.rowCount];

            for (int i = 0; i < this.items.Length; i++)
            {
                this.items[i] = stream.ReadInt64();
            }
        }

        public override void Persist(BinaryWriter destination)
        {
            destination.Write(this.pageId);
            destination.Write(this.pageSize);
            destination.Write((int)this.PageType());
            destination.Write(this.rowCount);
            destination.Write(this.prevPageId);
            destination.Write(this.nextPageId);

            for (int i = 0; i < this.rowCount; i++)
            {
                destination.Write(this.items[i]);
            }
        }

        public override void RedoLog(ILogRecord record, ITransaction tran)
        {
            var redoContent = record.GetRedoContent();

            if (record.GetRecordType() == LogRecordType.RowModify)
            {
                this.items[redoContent.RowPosition] = BitConverter.ToInt64(redoContent.DataToApply);
            }
            else if (record.GetRecordType() == LogRecordType.RowInsert)
            {
                if (redoContent.RowPosition != items.Length)
                {
                    throw new LogCorruptedException();
                }

                // TODO: Perf is terrible.
                // Maybe list is better choice for values?
                long val = BitConverter.ToInt64(redoContent.DataToApply);
                this.items = this.items.Concat(new long[1] { val }).ToArray();
                this.rowCount = (uint)this.items.Length;
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
                this.items[undoContent.RowPosition] = BitConverter.ToInt64(undoContent.DataToUndo);
            }
            else if (record.GetRecordType() == LogRecordType.RowInsert)
            {
                if (this.items.Length != undoContent.RowPosition + 1)
                {
                    throw new LogCorruptedException();
                }

                this.items = this.items.Take(this.items.Length - 1).ToArray();
                this.rowCount = (uint)this.items.Length;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        protected override byte[] SerializeItem(long item) => BitConverter.GetBytes(item);
    }
}
