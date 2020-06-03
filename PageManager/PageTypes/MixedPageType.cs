using LogManager;
using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;

namespace PageManager
{
    public interface IAllocateMixedPage
    {
        MixedPage AllocateMixedPage(ColumnType[] columnTypes, ulong prevPage, ulong nextPage, ITransaction tran);
        MixedPage GetMixedPage(ulong pageId, ITransaction tran);
    }

    public class MixedPage : PageSerializerBase<RowsetHolder>
    {
        private readonly ColumnType[] columnTypes;

        public MixedPage(uint pageSize, ulong pageId, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId, ITransaction tran)
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
            this.items = new RowsetHolder(this.columnTypes);

            ILogRecord logRecord = new AllocatePageLogRecord(pageId, tran.TranscationId(), PageManager.PageType.MixedPage, pageSize, nextPageId, prevPageId, columnTypes);
            tran.AddRecord(logRecord);
        }

        public MixedPage(BinaryReader stream, ColumnType[] columnTypes)
        {
            this.columnTypes = columnTypes;

            this.pageId = stream.ReadUInt64();
            this.pageSize = stream.ReadUInt32();

            PageType pageTypePersisted = (PageType)stream.ReadUInt32();

            if (PageManager.PageType.MixedPage != pageTypePersisted)
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

            this.items = new RowsetHolder(this.columnTypes);

            this.items.Deserialize(stream, this.rowCount);

            if (this.items.GetRowCount() != this.rowCount)
            {
                throw new SerializationException();
            }
        }

        public override PageType PageType() => PageManager.PageType.MixedPage;

        public override RowsetHolder Fetch()
        {
            return this.items;
        }

        public override void Merge(RowsetHolder item, ITransaction transaction)
        {
            uint prevSize = this.items.GetRowCount();
            this.items.Merge(item);
            this.rowCount = this.items.GetRowCount();

            byte[] lrContent = new byte[item.StorageSizeInBytes()];
            using (MemoryStream ms = new MemoryStream(lrContent))
            using (BinaryWriter bw = new BinaryWriter(ms))
            {
                bw.Write((ushort)item.GetRowCount());
                item.Serialize(bw);
            }

            ILogRecord rc = new InsertRowRecord(this.pageId, (ushort)(prevSize), lrContent, transaction.TranscationId());
            transaction.AddRecord(rc);
        }

        public override uint MaxRowCount()
        {
            return (this.pageSize - IPage.FirstElementPosition - sizeof(int)) / RowsetHolder.CalculateSizeOfRow(this.columnTypes);
        }

        public override bool CanFit(RowsetHolder items)
        {
            int freeSpace = (int)((this.pageSize - IPage.FirstElementPosition) - (this.RowCount() * RowsetHolder.CalculateSizeOfRow(this.columnTypes)));
            return freeSpace >= items.StorageSizeInBytes();
        }

        public override uint GetSizeNeeded(RowsetHolder items)
        {
            return items.StorageSizeInBytes();
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

                this.items.Serialize(bw);
            }
        }

        public override void RedoLog(ILogRecord record, ITransaction tran)
        {
            var redoContent = record.GetRedoContent();

            using (MemoryStream ms = new MemoryStream(redoContent.DataToApply))
            using (BinaryReader br = new BinaryReader(ms))
            {
                ushort rsCount = br.ReadUInt16();
                var rs = new RowsetHolder(this.columnTypes);
                rs.Deserialize(br, rsCount);

                if (record.GetRecordType() == LogRecordType.RowModify)
                {
                    this.items.ModifyRow(redoContent.RowPosition, rs);
                }
                else if (record.GetRecordType() == LogRecordType.RowInsert)
                {
                    if (redoContent.RowPosition != items.GetRowCount())
                    {
                        throw new LogCorruptedException();
                    }

                    this.items.Merge(rs);
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        public override void UndoLog(ILogRecord record, ITransaction tran)
        {
            var undoContent = record.GetUndoContent();

            if (record.GetRecordType() == LogRecordType.RowModify)
            {
                using (MemoryStream ms = new MemoryStream(undoContent.DataToUndo))
                using (BinaryReader br = new BinaryReader(ms))
                {
                    ushort rsCount = br.ReadUInt16();
                    var rs = new RowsetHolder(this.columnTypes);
                    rs.Deserialize(br, rsCount);
                    this.items.ModifyRow(undoContent.RowPosition, rs);
                }
            }
            else if (record.GetRecordType() == LogRecordType.RowInsert)
            {
                if (undoContent.RowPosition + 1 != items.GetRowCount())
                {
                    throw new LogCorruptedException();
                }

                this.items.RemoveRow(undoContent.RowPosition);
                this.rowCount--;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public override bool Equals(PageSerializerBase<RowsetHolder> other)
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

            if (!this.Fetch().Equals(other.Fetch()))
            {
                return false;
            }

            return true;
        }
    }
}
