using DataStructures;
using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpScan : IPhysicalOperator<RowHolder>
    {
        private readonly IPageCollection<RowHolder> source;
        private readonly ITransaction tran;
        private readonly MetadataColumn[] scanColumnInfo;

        public PhyOpScan(IPageCollection<RowHolder> collection, ITransaction tran, MetadataColumn[] scanColumnInfo)
        {
            this.source = collection;
            this.tran = tran;
            this.scanColumnInfo = scanColumnInfo;
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            await foreach (RowHolder rowHolder in this.source.Iterate(tran))
            {
                yield return rowHolder;
            }
        }

        public Task Invoke()
        {
            throw new NotImplementedException();
        }

        public MetadataColumn[] GetOutputColumns() => this.scanColumnInfo;
    }
}
