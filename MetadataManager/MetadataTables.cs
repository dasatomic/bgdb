using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace MetadataManager
{
    public struct MetadataTable
    {
        public int TableId;
        public string TableName;
        public ulong RootPage;
    }

    public struct TableCreateDefinition
    {
        public string TableName;
        public string[] ColumnNames;
        public ColumnType[] ColumnTypes;
    }

    public class MetadataTablesManager : IMetadataObjectManager<MetadataTable, TableCreateDefinition>
    {
        public const string MetadataTableName = "sys.tables";

        private PageListCollection pageListCollection;
        private HeapWithOffsets<char[]> stringHeap;
        private IMetadataObjectManager<MetadataColumn, ColumnCreateDefinition> columnManager;
        private IAllocateMixedPage pageAllocator;

        private static ColumnType[] columnDefinitions = new ColumnType[]
        {
            ColumnType.Int,
            ColumnType.StringPointer,
            ColumnType.PagePointer,
        };

        public static ColumnType[] GetSchemaDefinition() => columnDefinitions;

        public MetadataTablesManager(IAllocateMixedPage pageAllocator, MixedPage firstPage, HeapWithOffsets<char[]> stringHeap, IMetadataObjectManager<MetadataColumn, ColumnCreateDefinition> columnManager)
        {
            if (pageAllocator == null || firstPage == null || columnManager == null)
            {
                throw new ArgumentNullException();
            }

            this.pageListCollection = new PageListCollection(pageAllocator, columnDefinitions, firstPage);
            this.stringHeap = stringHeap;
            this.columnManager = columnManager;
            this.pageAllocator = pageAllocator;
        }

        public bool Exists(TableCreateDefinition def)
        {
            foreach (RowsetHolder rh in pageListCollection)
            {
                foreach (PagePointerOffsetPair stringPointer in rh.GetStringPointerColumn(0))
                {
                    if (def.TableName == new string(stringHeap.Fetch(stringPointer)))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public int CreateObject(TableCreateDefinition def)
        {
            if (this.Exists(def))
            {
                throw new ElementWithSameNameExistsException();
            }

            if (def.ColumnNames.Length != def.ColumnTypes.Length)
            {
                throw new ArgumentException();
            }

            int id = 1;
            if (!pageListCollection.IsEmpty())
            {
                int maxId = pageListCollection.Max<int>(rh => rh.GetIntColumn(0).Max(), startMin: 0);
                id = maxId + 1;
            }

            MixedPage rootPage = this.pageAllocator.AllocateMixedPage(def.ColumnTypes, 0, 0);

            RowsetHolder rh = new RowsetHolder(columnDefinitions);
            PagePointerOffsetPair namePointer =  this.stringHeap.Add(def.TableName.ToCharArray());

            rh.SetColumns(new int[1][] { new[] { id } }, new double[0][], new PagePointerOffsetPair[1][] { new[] { namePointer } }, new long[1][] { new[] { (long)rootPage.PageId() }});
            pageListCollection.Add(rh);

            for (int i = 0; i < def.ColumnNames.Length; i++)
            {
                ColumnCreateDefinition ccd = new ColumnCreateDefinition()
                {
                    ColumnName = def.ColumnNames[i],
                    ColumnType = def.ColumnTypes[i],
                    TableId = id,
                };

                columnManager.CreateObject(ccd);
            }


            return id;
        }

        public IEnumerator<MetadataTable> GetEnumerator()
        {
            foreach (RowsetHolder rh in pageListCollection)
            {
                for (int i = 0; i < rh.GetRowCount(); i++)
                {
                    var mdObj = 
                        new MetadataTable()
                        {
                            TableId = rh.GetIntColumn(0)[i],
                            RootPage = (ulong)rh.GetPagePointerColumn(0)[i],
                        };

                    rh.GetStringPointerColumn(0);
                    PagePointerOffsetPair stringPointer = rh.GetStringPointerColumn(0)[i];
                    char[] tableName= this.stringHeap.Fetch(stringPointer);

                    mdObj.TableName = new string(tableName);

                    yield return mdObj;
                }
            }
        }

        public MetadataTable GetById(int id)
        {
            foreach (var table in this)
            {
                if (table.TableId == id)
                {
                    return table;
                }
            }

            throw new KeyNotFoundException();
        }

        private IEnumerator GetEnumerator1()
        {
            return this.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator1();
        }
    }
}
