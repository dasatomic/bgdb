using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MetadataManager
{
    public class MetadataTable
    {
        public int TableId;
        public string TableName;
    }

    public class MetadataTablesManager : IMetadataObjectManager
    {
        public const string MetadataTableName = "sys.tables";

        private PageListCollection pageListCollection;
        private HeapWithOffsets<char[]> stringHeap;

        private readonly ColumnType[] columnDefinitions = new ColumnType[]
        {
            ColumnType.Int,
            ColumnType.StringPointer,
        };

        public ColumnType[] GetSchemaDefinition() => columnDefinitions;

        public MetadataTablesManager(IAllocateMixedPage pageAllocator, MixedPage firstPage, HeapWithOffsets<char[]> stringHeap)
        {
            if (pageAllocator == null || firstPage == null)
            {
                throw new ArgumentNullException();
            }

            pageListCollection = new PageListCollection(pageAllocator, this.columnDefinitions, firstPage);
            this.stringHeap = stringHeap;
        }

        public int CreateObject(string tableName)
        {
            // TODO: Check if table name already exists...

            int maxId = pageListCollection.Max<int>(rh => rh.GetIntColumn(0).Max(), startMin: 0);
            int id = maxId + 1;

            RowsetHolder rh = new RowsetHolder(this.columnDefinitions);
            PagePointerOffsetPair namePointer =  this.stringHeap.Add(tableName.ToCharArray());

            rh.SetColumns(new int[1][] { new[] { id } }, null, new PagePointerOffsetPair[1][] { new[] { namePointer } }, null);
            pageListCollection.Add(rh);

            return id;
        }
    }
}
