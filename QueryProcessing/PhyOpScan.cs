using DataStructures;
using PageManager;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpScan : IPhysicalOperator<RowHolderFixed>
    {
        private readonly PageListCollection source;
        private readonly HeapWithOffsets<char[]> strHeap = null;
        private readonly ITransaction tran;

        public PhyOpScan(PageListCollection collection, HeapWithOffsets<char[]> strHeap, ITransaction tran)
        {
            this.source = collection;
            this.strHeap = strHeap;
            this.tran = tran;
        }

        public async IAsyncEnumerable<RowHolderFixed> Iterate(ITransaction tran)
        {
            await foreach (RowHolderFixed rowHolder in this.source.Iterate(tran))
            {
                yield return rowHolder;
            }
        }

        public Task Invoke()
        {
            throw new NotImplementedException();
        }
    }
}
