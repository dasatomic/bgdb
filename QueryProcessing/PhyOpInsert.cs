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

        public PhyOpTableInsert(MetadataTable mdTable, IAllocateMixedPage pageAllocator, HeapWithOffsets<char[]> stringHeap, IPhysicalOperator<Row> input)
        {
            this.mdTable = mdTable;
            this.pageAllocator = pageAllocator;

            IPage rootPage = this.pageAllocator.GetMixedPage(this.mdTable.RootPage);
            this.pageCollection = new PageListCollection(this.pageAllocator, mdTable.Columns.Select(c => c.ColumnType).ToArray(), rootPage);
            this.stringHeap = stringHeap;
            this.input = input;
        }

        public void Invoke()
        {
            foreach (Row row in this.input)
            {
                this.pageCollection.Add(row.ToRowsetHolder(mdTable.Columns.Select(c => c.ColumnType).ToArray(), stringHeap));
            }
        }

        public IEnumerator<Row> GetEnumerator()
        {
            yield break;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            yield break;
        }
    }
}
