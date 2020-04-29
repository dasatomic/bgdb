using System;
using System.Runtime.Serialization;

namespace PageManager
{
    class MixedPage : IPageSerializer<RowsetHolder>

    {
        private readonly uint pageSize;
        private readonly ulong pageId;

        // Byte representation:
        // [0-7] PageId
        // [8-11] PageSize
        // [12-15] PageType
        protected byte[] content;

        private readonly ColumnType[] columnTypes;

        protected const uint PageIdPosition = 0;
        protected const uint PageSizePosition = 8;
        protected const uint PageTypePosition = 12;
        protected const uint NumOfRowsPosition = 16;
        protected const uint FirstElementPosition = 20;

        public MixedPage(uint pageSize, ulong pageId, ColumnType[] columnTypes)
        {
            this.pageSize = pageSize;
            this.pageId = pageId;

            this.content = new byte[pageSize];
            this.columnTypes = columnTypes;
        }

        public byte[] GetContent() => this.content;

        public ulong PageId() => this.pageId;

        public PageType PageType() => PageManager.PageType.MixedPage;

        public uint SizeInBytes() => this.pageSize;

        public void Serialize(RowsetHolder items)
        {
            uint neededSize = this.GetSizeNeeded(items);
            if (this.MaxRowCount() < neededSize)
            {
                throw new SerializationException();
            }

            uint contentPosition = 0;
            foreach (byte pageByte in BitConverter.GetBytes(this.pageId))
            {
                content[contentPosition] = pageByte;
                contentPosition++;
            }

            foreach (byte sizeByte in BitConverter.GetBytes(this.pageSize))
            {
                content[contentPosition] = sizeByte;
                contentPosition++;
            }

            foreach (byte typeByte in BitConverter.GetBytes((int)PageManager.PageType.MixedPage))
            {
                content[contentPosition] = typeByte;
                contentPosition++;
            }

            foreach (byte numOfRowsByte in BitConverter.GetBytes(neededSize))
            {
                content[contentPosition] = numOfRowsByte;
                contentPosition++;
            }

            SerializeInternal(items);
        }

        private void SerializeInternal(RowsetHolder item)
        {
            item.SerializeInto(this.content.AsSpan((int)FirstElementPosition));
        }

        private uint GetSizeNeeded(RowsetHolder items)
        {
            return items.StorageSizeInBytes();
        }

        public RowsetHolder Deserialize()
        {
            RowsetHolder rowsetHolder = new RowsetHolder(this.columnTypes);
            rowsetHolder.Deserialize(this.content.AsSpan((int)FirstElementPosition));
            return rowsetHolder;
        }

        public uint MaxRowCount()
        {
            throw new System.NotImplementedException();
        }
    }
}
