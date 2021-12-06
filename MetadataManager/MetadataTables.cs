using DataStructures;
using MetadataManager.Exceptions;
using PageManager;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace MetadataManager
{
    public struct MetadataTable
    {
        public const int TableIdColumnPos = 0;
        public int TableId;
        public const int TableNameColumnPos = 1;
        public string TableName;
        public const int RootPageColumnPos = 2;
        public ulong RootPage;
        public MetadataColumn[] Columns;

        public IPageCollection<RowHolder> Collection;
    }

    public struct TableCreateDefinition
    {
        public string TableName;
        public string[] ColumnNames;
        public ColumnInfo[] ColumnTypes;
        public int[] ClusteredIndexPositions;
    }

    public class MetadataTablesManager : IMetadataObjectManager<MetadataTable, TableCreateDefinition, int>
    {
        public const string MetadataTableName = "sys.tables";

        private IPageCollection<RowHolder> pageListCollection;
        private HeapWithOffsets<char[]> stringHeap;
        private IMetadataObjectManager<MetadataColumn, ColumnCreateDefinition, Tuple<int, int>> columnManager;
        private IAllocateMixedPage pageAllocator;

        private static ColumnInfo[] columnDefinitions = new ColumnInfo[]
        {
            new ColumnInfo(ColumnType.Int),
            new ColumnInfo(ColumnType.String, 20),
            new ColumnInfo(ColumnType.PagePointer),
        };

        private object cacheLock = new object();
        private Dictionary<string, MetadataTable> nameTableCache = new Dictionary<string, MetadataTable>();

        public static ColumnInfo[] GetSchemaDefinition() => columnDefinitions;

        public MetadataTablesManager(IAllocateMixedPage pageAllocator, MixedPage firstPage, HeapWithOffsets<char[]> stringHeap, IMetadataObjectManager<MetadataColumn, ColumnCreateDefinition, Tuple<int, int>> columnManager)
        {
            if (pageAllocator == null || firstPage == null || columnManager == null)
            {
                throw new ArgumentNullException();
            }

            this.pageListCollection = new PageListCollection(pageAllocator, columnDefinitions, firstPage.PageId());
            this.stringHeap = stringHeap;
            this.columnManager = columnManager;
            this.pageAllocator = pageAllocator;
        }

        public async Task<bool> Exists(TableCreateDefinition def, ITransaction tran)
        {
            await foreach (RowHolder rh in pageListCollection.Iterate(tran))
            {
                PagePointerOffsetPair stringPointer = rh.GetField<PagePointerOffsetPair>(1);

                if (CharrArray.Compare(def.TableName, await stringHeap.Fetch(stringPointer, tran)) == 0)
                {
                    return true;
                }
            }

            return false;
        }

        public async Task<int> CreateObject(TableCreateDefinition def, ITransaction tran)
        {
            if (await this.Exists(def, tran))
            {
                throw new ElementWithSameNameExistsException();
            }

            if (def.ColumnNames.Length != def.ColumnTypes.Length)
            {
                throw new ArgumentException();
            }

            int id = 1;
            if (!(await this.pageListCollection.IsEmpty(tran)))
            {
                int maxId = await this.pageListCollection.Max<int>(rh => rh.GetField<int>(0), startMin: 0, tran);
                id = maxId + 1;
            }

            bool isClusteredIndexCollection = def.ClusteredIndexPositions.Any(pos => pos != -1);
            CollectionType collectionType = isClusteredIndexCollection ? CollectionType.BTree : CollectionType.PageList;

            MixedPage rootPage = await CollectionHandler.CreateInitialPage(collectionType, def.ColumnTypes, this.pageAllocator, tran);

            RowHolder rh = new RowHolder(columnDefinitions);
            PagePointerOffsetPair namePointer =  await this.stringHeap.Add(def.TableName.ToCharArray(), tran);

            rh.SetField<int>(0, id);
            rh.SetField<PagePointerOffsetPair>(1, namePointer);
            rh.SetField<long>(2, (long)rootPage.PageId());
            await pageListCollection.Add(rh, tran);

            for (int i = 0; i < def.ColumnNames.Length; i++)
            {
                int posInClusteredIndex = -1;

                {
                    int currClusteredIndexIter = 0;
                    foreach (int clusteredIndex in def.ClusteredIndexPositions)
                    {
                        if (clusteredIndex == i)
                        {
                            // this column is part of clustered index.
                            posInClusteredIndex = currClusteredIndexIter;
                            break;
                        }

                        currClusteredIndexIter++;
                    }
                }

                ColumnCreateDefinition ccd = new ColumnCreateDefinition(id, def.ColumnNames[i], def.ColumnTypes[i], i, posInClusteredIndex);
                await columnManager.CreateObject(ccd, tran);
            }

            return id;
        }

        public async IAsyncEnumerable<MetadataTable> Iterate(ITransaction tran)
        {
            await foreach (RowHolder rh in pageListCollection.Iterate(tran))
            {
                var mdObj = 
                    new MetadataTable()
                    {
                        TableId = rh.GetField<int>(MetadataTable.TableIdColumnPos),
                        RootPage = (ulong)rh.GetField<long>(MetadataTable.RootPageColumnPos),
                    };

                PagePointerOffsetPair stringPointer = rh.GetField<PagePointerOffsetPair>(MetadataTable.TableNameColumnPos);
                char[] tableName= await this.stringHeap.Fetch(stringPointer, tran);

                mdObj.TableName = new string(tableName);

                List<MetadataColumn> columns = new List<MetadataColumn>();
                await foreach (var column in this.columnManager.Iterate(tran))
                {
                    if (column.TableId == mdObj.TableId)
                    {
                        columns.Add(column);
                    }
                }

                mdObj.Columns = columns.ToArray();

                // TODO: Add some better way to make distinction between btrees and heaps.
                if (mdObj.Columns.Any(col => col.ClusteredIndexPart != MetadataColumn.NotPartOfClusteredIndex))
                {
                    // Find the column that has the index.
                    MetadataColumn[] clusteredIndexPositionsOrdered =
                        mdObj.Columns.Where(cl => cl.ClusteredIndexPart != MetadataColumn.NotPartOfClusteredIndex).OrderBy(cl => cl.ClusteredIndexPart).ToArray();

                    if (clusteredIndexPositionsOrdered.Length > 1)
                    {
                        throw new OnlyOneClusteredIndexSupportedException();
                    }

                    MetadataColumn clusteredIndexColumn = clusteredIndexPositionsOrdered[0];

                    // find this column in parent table.
                    int columnPos = 0;
                    for (; columnPos < mdObj.Columns.Length; columnPos++)
                    {
                        if (clusteredIndexColumn.ColumnId == mdObj.Columns[columnPos].ColumnId)
                        {
                            break;
                        }
                    }

                    Debug.Assert(clusteredIndexColumn.ClusteredIndexPart == 0);

                    Func<RowHolder, RowHolder, int> indexComparer = ColumnTypeHandlerRouter<Func<RowHolder, RowHolder, int>>.Route(
                        new BtreeCompareFunctionCreator() { IndexColumnPosition = columnPos },
                        clusteredIndexColumn.ColumnType.ColumnType);

                    mdObj.Collection = new BTreeCollection(
                        this.pageAllocator,
                        mdObj.Columns.Select(ci => ci.ColumnType).ToArray(),
                        indexComparer,
                        columnPos,
                        mdObj.RootPage,
                        tran);
                }
                else
                {
                    // Just heap/page list.
                    mdObj.Collection = new PageListCollection(this.pageAllocator, mdObj.Columns.Select(ci => ci.ColumnType).ToArray(), mdObj.RootPage);
                }

                yield return mdObj;
            }
        }

        public async Task<MetadataTable> GetById(int id, ITransaction tran)
        {
            await foreach (var table in this.Iterate(tran))
            {
                if (table.TableId == id)
                {
                    return table;
                }
            }

            throw new KeyNotFoundException();
        }

        public async Task<MetadataTable> GetByName(string name, ITransaction tran)
        {
            string lookupName = name.ToUpper();
            lock (this.cacheLock)
            {
                MetadataTable md;
                if (this.nameTableCache.TryGetValue(lookupName, out md))
                {
                    return md;
                }
            }

            await foreach (var table in this.Iterate(tran))
            {
                lock (this.cacheLock)
                {
                    this.nameTableCache[table.TableName] = table;
                }

                if (table.TableName == lookupName)
                {
                    return table;
                }
            }

            throw new KeyNotFoundException();
        }
    }

    public class BtreeCompareFunctionCreator : ColumnTypeHandlerBasicDouble<Func<RowHolder, RowHolder, int>>
    {
        public int IndexColumnPosition { get; init; }

        public Func<RowHolder, RowHolder, int> HandleDouble()
        {
            return (RowHolder rh1, RowHolder rh2) =>
            {
                return rh1.GetField<double>(this.IndexColumnPosition).CompareTo(rh2.GetField<double>(this.IndexColumnPosition));
            };
        }

        public Func<RowHolder, RowHolder, int> HandleInt()
        {
            return (RowHolder rh1, RowHolder rh2) =>
            {
                return rh1.GetField<int>(this.IndexColumnPosition).CompareTo(rh2.GetField<int>(this.IndexColumnPosition));
            };
        }

        public Func<RowHolder, RowHolder, int> HandleString()
        {
            return (RowHolder rh1, RowHolder rh2) =>
            {
                return CharrArray.Compare(rh1.GetStringField(this.IndexColumnPosition), rh2.GetStringField(this.IndexColumnPosition));
            };
        }
    }
}
