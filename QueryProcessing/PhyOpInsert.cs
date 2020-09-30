using DataStructures;
using MetadataManager;
using PageManager;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpTableInsert : IPhysicalOperator<RowHolder>
    {
        private IPageCollection<RowHolder> pageCollection;
        private IPhysicalOperator<RowHolder> input;
        private ITransaction tran;

        public PhyOpTableInsert(IPageCollection<RowHolder> pageCollection, IPhysicalOperator<RowHolder> input, ITransaction tran)
        {
            this.pageCollection = pageCollection;
            this.input = input;
            this.tran = tran;
        }

        public async Task Invoke()
        {
            await foreach (RowHolder row in this.input.Iterate(this.tran))
            {
                await this.pageCollection.Add(row, tran).ConfigureAwait(false);
            }
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            await Task.FromResult(0);
            yield break;
        }
    }
}
