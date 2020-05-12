using System;
using System.IO;
using System.Runtime.Serialization;

namespace PageManager
{
    public interface IAllocateMixedPage
    {
        MixedPage AllocateMixedPage(ColumnType[] columnTypes, ulong prevPage, ulong nextPage);
        MixedPage GetMixedPage(ulong pageId);
    }

    public class MixedPage : PageSerializerBase<RowsetHolder>
    {
        private readonly ColumnType[] columnTypes;

        public MixedPage(uint pageSize, ulong pageId, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId)
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

        public override void Merge(RowsetHolder item)
        {
            this.items.Merge(item);
            this.rowCount = this.items.GetRowCount();
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

        public override void Store(RowsetHolder items)
        {
            this.items = items;
            this.rowCount = this.items.GetRowCount();
        }

        public override void Persist(Stream destination)
        {
            throw new NotImplementedException();
        }
    }
}
