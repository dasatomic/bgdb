using System;

namespace PageManager
{
    public interface IAllocateMixedPage
    {
        MixedPage AllocateMixedPage(ColumnType[] columnTypes);
        MixedPage GetMixedPage(ulong pageId);
    }

    public class MixedPage : PageSerializerBase<RowsetHolder>
    {
        private readonly ColumnType[] columnTypes;

        public MixedPage(uint pageSize, ulong pageId, ColumnType[] columnTypes)
        {
            if (columnTypes == null || columnTypes.Length == 0)
            {
                throw new ArgumentException("Column type definition can't be null or empty");
            }

            this.pageSize = pageSize;
            this.pageId = pageId;

            this.content = new byte[pageSize];
            this.columnTypes = columnTypes;
        }

        public override PageType PageType() => PageManager.PageType.MixedPage;

        protected override void SerializeInternal(RowsetHolder item)
        {
            item.SerializeInto(this.content.AsSpan((int)IPage.FirstElementPosition));
        }

        public override RowsetHolder Deserialize()
        {
            RowsetHolder rowsetHolder = new RowsetHolder(this.columnTypes);

            int elemCount = BitConverter.ToInt32(this.content, (int)IPage.FirstElementPosition);
            int size = (int)(elemCount * RowsetHolder.CalculateSizeOfRow(this.columnTypes) + sizeof(int));
            rowsetHolder.Deserialize(this.content.AsSpan((int)IPage.FirstElementPosition, size));

            return rowsetHolder;
        }

        public override uint MaxRowCount()
        {
            return (this.pageSize - IPage.FirstElementPosition - sizeof(int)) / RowsetHolder.CalculateSizeOfRow(this.columnTypes);
        }

        public override bool CanFit(RowsetHolder items)
        {
            return this.pageSize - IPage.FirstElementPosition >= items.StorageSizeInBytes();
        }

        public override uint GetSizeNeeded(RowsetHolder items)
        {
            return items.StorageSizeInBytes();
        }

        protected override uint GetRowCount(RowsetHolder items)
        {
            return items.GetRowCount();
        }
    }
}
