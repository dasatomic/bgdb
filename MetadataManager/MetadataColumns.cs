using DataStructures;
using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

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
        public const int ColumnTypeLength = 4;
        public ColumnInfo ColumnType;
    }

    /// <summary>
    ///  Definition of one column.
    ///  Table id and column id are unique identifiers.
    /// </summary>
    public struct ColumnCreateDefinition
    {
        public ColumnCreateDefinition(int tableId, string columnName, ColumnInfo columnInfo, int columnId)
        {
            this.TableId = tableId;
            this.ColumnName = columnName;
            this.ColumnType = columnInfo;
            this.ColumnId = columnId;
        }

        /// <summary>
        /// Table id this column belongs to.
        /// </summary>
        public int TableId { get; }

        /// <summary>
        /// Name of this column.
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// Type of this column.
        /// </summary>
        public ColumnInfo ColumnType { get; }

        /// <summary>
        /// Column position in table.
        /// </summary>
        public int ColumnId { get; }
    }

    public class MetadataColumnsManager : IMetadataObjectManager<MetadataColumn, ColumnCreateDefinition, Tuple<int, int> /* column id - table id */>
    {
        public const string MetadataTableName = "sys.columns";

        private IPageCollection<RowHolderFixed> pageListCollection;
        private HeapWithOffsets<char[]> stringHeap;

        private const int MAX_NAME_LENGTH = 20;

        private static ColumnInfo[] columnDefinitions = new ColumnInfo[]
        {
            new ColumnInfo(ColumnType.Int), // Column id
            new ColumnInfo(ColumnType.Int), // Table id
            new ColumnInfo(ColumnType.String, MAX_NAME_LENGTH), // pointer to name
            new ColumnInfo(ColumnType.Int), // column type
            new ColumnInfo(ColumnType.Int), // column type length
        };

        public static ColumnInfo[] GetSchemaDefinition() => columnDefinitions;

        public MetadataColumnsManager(IAllocateMixedPage pageAllocator, MixedPage firstPage, HeapWithOffsets<char[]> stringHeap)
        {
            if (pageAllocator == null || firstPage == null)
            {
                throw new ArgumentNullException();
            }

            this.pageListCollection = new PageListCollection(pageAllocator, columnDefinitions, firstPage.PageId());
            this.stringHeap = stringHeap;
        }

        public async IAsyncEnumerable<MetadataColumn> Iterate(ITransaction tran)
        {
            await foreach (RowHolderFixed rh in pageListCollection.Iterate(tran))
            {
                var mdObj =
                    new MetadataColumn()
                    {
                        ColumnId = rh.GetField<int>(MetadataColumn.ColumnIdColumnPos),
                        TableId = rh.GetField<int>(MetadataColumn.TableIdColumnPos),
                        ColumnType = new ColumnInfo((ColumnType)rh.GetField<int>(MetadataColumn.ColumnTypeColumnPos), rh.GetField<int>(MetadataColumn.ColumnTypeLength))
                    };

                PagePointerOffsetPair stringPointer = rh.GetField<PagePointerOffsetPair>(MetadataColumn.ColumnNameColumnPos);
                char[] columnName = await this.stringHeap.Fetch(stringPointer, tran);
                mdObj.ColumnName = new string(columnName);

                yield return mdObj;
            }
        }

        public async Task<int> CreateObject(ColumnCreateDefinition def, ITransaction tran)
        {
            if (await this.Exists(def, tran))
            {
                throw new ElementWithSameNameExistsException();
            }

            RowHolderFixed rh = new RowHolderFixed(columnDefinitions);
            PagePointerOffsetPair namePointer =  await this.stringHeap.Add(def.ColumnName.ToCharArray(), tran);

            rh.SetField<int>(0, def.ColumnId);
            rh.SetField<int>(1, def.TableId);
            rh.SetField<PagePointerOffsetPair>(2, namePointer);
            rh.SetField<int>(3, (int)def.ColumnType.ColumnType);
            rh.SetField<int>(4, def.ColumnType.RepCount);

            await pageListCollection.Add(rh, tran);

            return def.ColumnId;
        }

        public async Task<bool> Exists(ColumnCreateDefinition def, ITransaction tran)
        {
            await foreach (RowHolderFixed rh in pageListCollection.Iterate(tran))
            {
                int tableId = rh.GetField<int>(MetadataColumn.TableIdColumnPos);

                if (tableId == def.TableId)
                {
                    PagePointerOffsetPair stringPointer = rh.GetField<PagePointerOffsetPair>(MetadataColumn.ColumnNameColumnPos);

                    if (def.ColumnName == new string(await stringHeap.Fetch(stringPointer, tran)))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public async Task<MetadataColumn> GetById(Tuple<int, int> id, ITransaction tran)
        {
            await foreach (var column in this.Iterate(tran))
            {
                if (column.ColumnId == id.Item1 && column.TableId == id.Item2)
                {
                    return column;
                }
            }

            throw new KeyNotFoundException();
        }
    }
}
