using PageManager;
using System;

namespace MetadataManager
{
    public class MetadataColumn
    {
        public int ColumnId;
        public int TableId;
        public string ColumnName;
        public ColumnType ColumnType;
    }

    public class MetadataColumnsManager : IMetadataObjectManager
    {
        public const string MetadataTableName = "sys.columns";

        private PageListCollection pageListCollection;

        private readonly ColumnType[] columnDefinitions = new ColumnType[]
        {
            ColumnType.Int,
            ColumnType.Int,
            ColumnType.StringPointer,
            ColumnType.Int,
        };

        public ColumnType[] GetSchemaDefinition() => columnDefinitions;

        public MetadataColumnsManager(IAllocateMixedPage pageAllocator, MixedPage firstPage)
        {
            if (pageAllocator == null || firstPage == null)
            {
                throw new ArgumentNullException();
            }

            pageListCollection = new PageListCollection(pageAllocator, this.columnDefinitions, firstPage);
        }
    }
}
