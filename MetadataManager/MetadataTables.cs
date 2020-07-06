using DataStructures;
using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;
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

            this.pageListCollection = new PageListCollection(pageAllocator, columnDefinitions, firstPage.PageId());
            this.stringHeap = stringHeap;
            this.columnManager = columnManager;
            this.pageAllocator = pageAllocator;
        }

        public async Task<bool> Exists(TableCreateDefinition def, ITransaction tran)
        {
            await foreach (RowsetHolder rh in pageListCollection.Iterate(tran))
            {
                foreach (PagePointerOffsetPair stringPointer in rh.GetStringPointerColumn(1))
                {
                    if (def.TableName == new string(await stringHeap.Fetch(stringPointer, tran)))
                    {
                        return true;
                    }
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
            if (!(await pageListCollection.IsEmpty(tran)))
            {
                int maxId = await pageListCollection.Max<int>(rh => rh.GetIntColumn(0).Max(), startMin: 0, tran);
                id = maxId + 1;
            }

            MixedPage rootPage = await this.pageAllocator.AllocateMixedPage(def.ColumnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);

            RowsetHolder rh = new RowsetHolder(columnDefinitions);
            PagePointerOffsetPair namePointer =  await this.stringHeap.Add(def.TableName.ToCharArray(), tran);

            rh.SetColumns(new int[1][] { new[] { id } }, new double[0][], new PagePointerOffsetPair[1][] { new[] { namePointer } }, new long[1][] { new[] { (long)rootPage.PageId() }});
            await pageListCollection.Add(rh, tran);

            for (int i = 0; i < def.ColumnNames.Length; i++)
            {
                ColumnCreateDefinition ccd = new ColumnCreateDefinition()
                {
                    ColumnName = def.ColumnNames[i],
                    ColumnType = def.ColumnTypes[i],
                    TableId = id,
                };

                await columnManager.CreateObject(ccd, tran);
            }


            return id;
        }

        public async IAsyncEnumerable<MetadataTable> Iterate(ITransaction tran)
        {
            await foreach (RowsetHolder rh in pageListCollection.Iterate(tran))
            {
                for (int i = 0; i < rh.GetRowCount(); i++)
                {
                    var mdObj = 
                        new MetadataTable()
                        {
                            TableId = rh.GetIntColumn(MetadataTable.TableIdColumnPos)[i],
                            RootPage = (ulong)rh.GetPagePointerColumn(MetadataTable.RootPageColumnPos)[i],
                        };

                    PagePointerOffsetPair stringPointer = rh.GetStringPointerColumn(MetadataTable.TableNameColumnPos)[i];
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

                    yield return mdObj;
                }
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
            await foreach (var table in this.Iterate(tran))
            {
                if (table.TableName == name)
                {
                    return table;
                }
            }

            throw new KeyNotFoundException();
        }
    }
}
