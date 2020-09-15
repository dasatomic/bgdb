using DataStructures;
using MetadataManager;
using PageManager;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpTableInsert : IPhysicalOperator<RowHolderFixed>
    {
        private MetadataTable mdTable;
        private PageListCollection pageCollection;
        private IAllocateMixedPage pageAllocator;
        private HeapWithOffsets<char[]> stringHeap;
        private IPhysicalOperator<RowHolderFixed> input;
        private ITransaction tran;

        public PhyOpTableInsert(MetadataTable mdTable, IAllocateMixedPage pageAllocator, HeapWithOffsets<char[]> stringHeap, IPhysicalOperator<RowHolderFixed> input, ITransaction tran)
        {
            this.mdTable = mdTable;
            this.pageAllocator = pageAllocator;

            ColumnInfo[] columnTypes = mdTable.Columns.Select(x => x.ColumnType).ToArray();

            this.pageCollection = new PageListCollection(this.pageAllocator, columnTypes, mdTable.RootPage);
            this.stringHeap = stringHeap;
            this.input = input;
            this.tran = tran;
        }

        public async Task Invoke()
        {
            await foreach (RowHolderFixed row in this.input.Iterate(this.tran))
            {
                await this.pageCollection.Add(row, tran).ConfigureAwait(false);
            }
        }

        public async IAsyncEnumerable<RowHolderFixed> Iterate(ITransaction tran)
        {
            await Task.FromResult(0);
            yield break;
        }
    }
}
