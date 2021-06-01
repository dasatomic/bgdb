using DataStructures;
using MetadataManager;
using PageManager;
using System.Collections.Generic;

namespace QueryProcessing
{
    public class PhyOpTableInsert : IPhysicalOperator<RowHolder>
    {
        private IPageCollection<RowHolder> pageCollection;
        private IPhysicalOperator<RowHolder> input;

        public PhyOpTableInsert(IPageCollection<RowHolder> pageCollection, IPhysicalOperator<RowHolder> input)
        {
            this.pageCollection = pageCollection;
            this.input = input;
        }

        public MetadataColumn[] GetOutputColumns() => new MetadataColumn[0];

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            await foreach (RowHolder row in this.input.Iterate(tran))
            {
                await this.pageCollection.Add(row, tran).ConfigureAwait(false);
            }

            yield break;
        }
    }
}
