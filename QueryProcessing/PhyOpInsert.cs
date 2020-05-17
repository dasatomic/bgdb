using MetadataManager;
using PageManager;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace QueryProcessing
{
    public class PhyOpTableInsert : IPhysicalOperator<Row>
    {
        private MetadataTable mdTable;
        private PageListCollection pageCollection;
        private IAllocateMixedPage pageAllocator;
        private HeapWithOffsets<char[]> stringHeap;
        private IPhysicalOperator<Row> input;
        private ITransaction tran;

        public PhyOpTableInsert(MetadataTable mdTable, IAllocateMixedPage pageAllocator, HeapWithOffsets<char[]> stringHeap, IPhysicalOperator<Row> input, ITransaction tran)
        {
            this.mdTable = mdTable;
            this.pageAllocator = pageAllocator;

            IPage rootPage = this.pageAllocator.GetMixedPage(this.mdTable.RootPage, tran);
            this.pageCollection = new PageListCollection(this.pageAllocator, mdTable.Columns.Select(c => c.ColumnType).ToArray(), rootPage);
            this.stringHeap = stringHeap;
            this.input = input;
            this.tran = tran;
        }

        public void Invoke()
        {
            foreach (Row row in this.input.Iterate(this.tran))
            {
                this.pageCollection.Add(row.ToRowsetHolder(mdTable.Columns.Select(c => c.ColumnType).ToArray(), stringHeap, tran), tran);
            }
        }

        public IEnumerable<Row> Iterate(ITransaction tran)
        {
            return Enumerable.Empty<Row>();
        }
    }
}
