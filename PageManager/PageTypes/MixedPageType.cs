using LogManager;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace PageManager
{
    public interface IAllocateMixedPage
    {
        Task<MixedPage> AllocateMixedPage(ColumnInfo[] columnTypes, ulong prevPage, ulong nextPage, ITransaction tran);
        Task<MixedPage> GetMixedPage(ulong pageId, ITransaction tran, ColumnInfo[] columnTypes);
    }

    public class MixedPage : PageSerializerBase<RowsetHolder, RowHolder>
    {
        private readonly ColumnInfo[] columnTypes;
        private Memory<byte> inMemoryStorage;

        public MixedPage(uint pageSize, ulong pageId, ColumnInfo[] columnTypes, ulong prevPageId, ulong nextPageId, Memory<byte> memory, ulong bufferPoolToken, ITransaction tran)
        {
            if (columnTypes == null || columnTypes.Length == 0)
            {
                throw new ArgumentException("Column type definition can't be null or empty");
            }

            this.pageSize = pageSize;
            this.pageId = pageId;

            this.columnTypes = columnTypes;
            this.prevPageId = prevPageId;
            this.nextPageId = nextPageId;
            this.inMemoryStorage = memory;
            this.bufferPoolToken = bufferPoolToken;
            this.items = new RowsetHolder(this.columnTypes, this.inMemoryStorage, init: true);

            ILogRecord logRecord = new AllocatePageLogRecord(pageId, tran.TranscationId(), global::PageManager.PageType.MixedPage, pageSize, nextPageId, prevPageId, columnTypes);
            tran.AddRecord(logRecord);

            this.isDirty = true;
        }

        public MixedPage(BinaryReader stream, Memory<byte> memory, ulong token, ColumnInfo[] columnInfos)
        {
            this.columnTypes = columnInfos;

            this.pageId = stream.ReadUInt64();
            this.pageSize = stream.ReadUInt32();

            PageType pageTypePersisted = (PageType)stream.ReadUInt32();

            if (global::PageManager.PageType.MixedPage != pageTypePersisted)
            {
                throw new InvalidCastException();
            }

            this.rowCount = stream.ReadUInt32();

            this.prevPageId = stream.ReadUInt64();
            this.nextPageId = stream.ReadUInt64();

            if (stream.BaseStream.Position % this.pageSize != IPage.FirstElementPosition)
            {
                throw new SerializationException();
            }

            this.inMemoryStorage = memory;
            this.bufferPoolToken = token;
            stream.Read(this.inMemoryStorage.Span);
            this.items = new RowsetHolder(this.columnTypes, this.inMemoryStorage, init: false);

            Debug.Assert(this.items.GetRowCount() == this.rowCount);

            this.isDirty = false;
        }

        public override PageType PageType() => global::PageManager.PageType.MixedPage;

        public override IEnumerable<RowHolder> Fetch(ITransaction tran)
        {
            tran.VerifyLock(this.pageId, LockManager.LockTypeEnum.Shared);
            return this.items.Iterate(this.columnTypes);
        }

        public override int Insert(RowHolder item, ITransaction transaction)
        {
            transaction.VerifyLock(this.pageId, LockManager.LockTypeEnum.Exclusive);

            int position = this.items.InsertRow(item);

            if (position == -1)
            {
                return position;
            }

            this.rowCount++;

            ILogRecord rc = new InsertRowRecord(this.pageId, (ushort)(position), item.Storage, transaction.TranscationId(), this.columnTypes, this.PageType());
            transaction.AddRecord(rc);

            this.isDirty = true;

            return position;
        }

        public override uint MaxRowCount() => this.items.MaxRowCount();

        public override bool CanFit(RowHolder item, ITransaction transaction)
        {
            transaction.VerifyLock(this.pageId, LockManager.LockTypeEnum.Shared);
            return this.items.FreeSpaceForItems() > 0;
        }

        public override void Persist(BinaryWriter destination)
        {
            Debug.Assert(this.PageType() == global::PageManager.PageType.MixedPage);
            Debug.Assert(this.rowCount == this.items.GetRowCount());

            destination.Write(this.pageId);
            destination.Write(this.pageSize);
            destination.Write((int)this.PageType());
            destination.Write(this.rowCount);
            destination.Write(this.prevPageId);
            destination.Write(this.nextPageId);

            destination.Write(this.inMemoryStorage.Span);
        }

        public override void RedoLog(ILogRecord record, ITransaction tran)
        {
            var redoContent = record.GetRedoContent();
            RowHolder rs = new RowHolder(this.columnTypes, redoContent.DataToApply);

            if (record.GetRecordType() == LogRecordType.RowModify)
            {
                this.items.SetRow(redoContent.RowPosition, rs);
            }
            else if (record.GetRecordType() == LogRecordType.RowInsert)
            {
                int ret = this.items.InsertRow(rs);
                Debug.Assert(ret != -1);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public override void UndoLog(ILogRecord record, ITransaction tran)
        {
            var undoContent = record.GetUndoContent();
            RowHolder rs = new RowHolder(this.columnTypes, undoContent.DataToUndo);

            if (record.GetRecordType() == LogRecordType.RowModify)
            {
                this.items.SetRow(undoContent.RowPosition, rs);
            }
            else if (record.GetRecordType() == LogRecordType.RowInsert)
            {
                this.items.DeleteRow(undoContent.RowPosition);
                this.rowCount--;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public override bool Equals(PageSerializerBase<RowsetHolder, RowHolder> other, ITransaction tran)
        {
            if (this.pageId != other.PageId())
            {
                return false;
            }

            if (this.MaxRowCount() != other.MaxRowCount())
            {
                return false;
            }

            if (this.PrevPageId() != other.PrevPageId())
            {
                return false;
            }

            if (this.NextPageId() != other.NextPageId())
            {
                return false;
            }

            if (!Enumerable.SequenceEqual(this.Fetch(tran), other.Fetch(tran)))
            {
                return false;
            }

            return true;
        }

        public override void Update(RowHolder item, ushort position, ITransaction transaction)
        {
            transaction.VerifyLock(this.pageId, LockManager.LockTypeEnum.Exclusive);

            RowHolder oldVal = new RowHolder(this.columnTypes);
            this.items.GetRow(position, ref oldVal);

            this.items.SetRow(position, item);

            ILogRecord rc = new UpdateRowRecord(this.pageId, (ushort)(position), diffOldValue: oldVal.Storage, diffNewValue: item.Storage, transaction.TranscationId(), this.columnTypes, this.PageType());
            transaction.AddRecord(rc);

            this.isDirty = true;
        }

        public override void At(ushort position, ITransaction tran, ref RowHolder item)
        {
            tran.VerifyLock(this.pageId, LockManager.LockTypeEnum.Shared);
            this.items.GetRow(position, ref item);
        }
    }
}
