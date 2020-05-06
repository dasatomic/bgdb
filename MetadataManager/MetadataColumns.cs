using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace MetadataManager
{
    public struct MetadataColumn
    {
        public const int ColumnIdColumnPos = 0;
        public int ColumnId;
        public const int TableIdColumnPos = 1;
        public int TableId;
        public const int ColumnNameColumnPos = 2;
        public string ColumnName;
        public const int ColumnTypeColumnPos = 3;
        public ColumnType ColumnType;
    }

    public struct ColumnCreateDefinition
    {
        public int TableId;
        public string ColumnName;
        public ColumnType ColumnType;
    }

    public class MetadataColumnsManager : IMetadataObjectManager<MetadataColumn, ColumnCreateDefinition>
    {
        public const string MetadataTableName = "sys.columns";

        private PageListCollection pageListCollection;
        private HeapWithOffsets<char[]> stringHeap;

        private static ColumnType[] columnDefinitions = new ColumnType[]
        {
            ColumnType.Int, // Column id
            ColumnType.Int, // Table id
            ColumnType.StringPointer, // pointer to name
            ColumnType.Int, // column type
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

        public IEnumerator<MetadataColumn> GetEnumerator()
        {
            foreach (RowsetHolder rh in pageListCollection)
            {
                for (int i = 0; i < rh.GetRowCount(); i++)
                {
                    var mdObj = 
                        new MetadataColumn()
                        {
                            ColumnId = rh.GetIntColumn(MetadataColumn.ColumnIdColumnPos)[i],
                            TableId = rh.GetIntColumn(MetadataColumn.TableIdColumnPos)[i],
                            ColumnType = (ColumnType)rh.GetIntColumn(MetadataColumn.ColumnTypeColumnPos)[i],
                        };

                    PagePointerOffsetPair stringPointer = rh.GetStringPointerColumn(MetadataColumn.ColumnNameColumnPos)[i];
                    char[] columnName = this.stringHeap.Fetch(stringPointer);

                    mdObj.ColumnName = new string(columnName);

                    yield return mdObj;
                }
            }
        }

        public int CreateObject(ColumnCreateDefinition def)
        {
            if (this.Exists(def))
            {
                throw new ElementWithSameNameExistsException();
            }

            int id = 0;
            if (!pageListCollection.IsEmpty())
            {
                int maxId = pageListCollection.Max<int>(rh => rh.GetIntColumn(MetadataColumn.ColumnIdColumnPos).Max(), startMin: 0);
                id = maxId + 1;
            }

            RowsetHolder rh = new RowsetHolder(columnDefinitions);
            PagePointerOffsetPair namePointer =  this.stringHeap.Add(def.ColumnName.ToCharArray());

            int[][] intCols = new int[3][];
            intCols[0] = new[] { id };
            intCols[1] = new[] { def.TableId };
            intCols[2] = new[] { (int)def.ColumnType };
            rh.SetColumns(intCols, new double[0][], new PagePointerOffsetPair[1][] { new[] { namePointer } }, new long[0][]);
            pageListCollection.Add(rh);

            return id;
        }

        public bool Exists(ColumnCreateDefinition def)
        {
            foreach (RowsetHolder rh in pageListCollection)
            {
                int[] tableIds = rh.GetIntColumn(MetadataColumn.TableIdColumnPos);

                for (int i = 0; i < tableIds.Length; i++)
                {
                    if (tableIds[i] == def.TableId)
                    {
                        PagePointerOffsetPair stringPointer = rh.GetStringPointerColumn(MetadataColumn.ColumnNameColumnPos)[i];

                        if (def.ColumnName == new string(stringHeap.Fetch(stringPointer)))
                        {
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        public MetadataColumn GetById(int id)
        {
            foreach (var column in this)
            {
                if (column.ColumnId == id)
                {
                    return column;
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
