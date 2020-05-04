using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MetadataManager
{
    public class MetadataColumn
    {
        public int ColumnId;
        public int TableId;
        public string ColumnName;
        public ColumnType ColumnType;
    }

    public class MetadataColumnsManager : IMetadataObjectManager<MetadataColumn>
    {
        public const string MetadataTableName = "sys.columns";

        private PageListCollection pageListCollection;
        private HeapWithOffsets<char[]> stringHeap;

        private static ColumnType[] columnDefinitions = new ColumnType[]
        {
            ColumnType.Int,
            ColumnType.Int,
            ColumnType.StringPointer,
            ColumnType.Int,
        };

        public static ColumnType[] GetSchemaDefinition() => columnDefinitions;

        public MetadataColumnsManager(IAllocateMixedPage pageAllocator, MixedPage firstPage, HeapWithOffsets<char[]> stringHeap)
        {
            if (pageAllocator == null || firstPage == null)
            {
                throw new ArgumentNullException();
            }

            this.pageListCollection = new PageListCollection(pageAllocator, columnDefinitions, firstPage);
            this.stringHeap = stringHeap;
        }

        public int CreateObject(int tableId, string columnName, ColumnType columnType)
        {
            // TODO: Check if column name already exists in this table...

            int maxId = pageListCollection.Max<int>(rh => rh.GetIntColumn(0).Max(), startMin: 0);
            int id = maxId + 1;

            RowsetHolder rh = new RowsetHolder(columnDefinitions);
            PagePointerOffsetPair namePointer =  this.stringHeap.Add(columnName.ToCharArray());

            rh.SetColumns(new int[1][] { new[] { id, tableId, (int)columnType } }, new double[0][], new PagePointerOffsetPair[1][] { new[] { namePointer } }, new long[0][]);
            pageListCollection.Add(rh);

            return id;
        }

        public IEnumerator<MetadataColumn> GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}
