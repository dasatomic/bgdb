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

    public class MetadataTablesManager : IMetadataObjectManager<MetadataTable>
    {
        public const string MetadataTableName = "sys.tables";

        private PageListCollection pageListCollection;
        private HeapWithOffsets<char[]> stringHeap;

        private static ColumnType[] columnDefinitions = new ColumnType[]
        {
            ColumnType.Int,
            ColumnType.StringPointer,
        };

        public static ColumnType[] GetSchemaDefinition() => columnDefinitions;

        public MetadataTablesManager(IAllocateMixedPage pageAllocator, MixedPage firstPage, HeapWithOffsets<char[]> stringHeap)
        {
            if (pageAllocator == null || firstPage == null)
            {
                throw new ArgumentNullException();
            }

            pageListCollection = new PageListCollection(pageAllocator, columnDefinitions, firstPage);
            this.stringHeap = stringHeap;
        }

        public bool Exists(string tableName)
        {
            foreach (RowsetHolder rh in pageListCollection)
            {
                foreach (PagePointerOffsetPair stringPointer in rh.GetStringPointerColumn(0))
                {
                    if (tableName == new string(stringHeap.Fetch(stringPointer)))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public int CreateObject(string tableName)
        {
            if (this.Exists(tableName))
            {
                throw new ElementWithSameNameExistsException();
            }

            int maxId = pageListCollection.Max<int>(rh => rh.GetIntColumn(0).Max(), startMin: 0);
            int id = maxId + 1;

            RowsetHolder rh = new RowsetHolder(columnDefinitions);
            PagePointerOffsetPair namePointer =  this.stringHeap.Add(tableName.ToCharArray());

            rh.SetColumns(new int[1][] { new[] { id } }, null, new PagePointerOffsetPair[1][] { new[] { namePointer } }, null);
            pageListCollection.Add(rh);

            return id;
        }

        public IEnumerator<MetadataTable> GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}
