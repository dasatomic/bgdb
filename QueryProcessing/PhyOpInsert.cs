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
        private IPageCollection<RowHolderFixed> pageCollection;
        private IPhysicalOperator<RowHolderFixed> input;
        private ITransaction tran;

        public PhyOpTableInsert(IPageCollection<RowHolderFixed> pageCollection, IPhysicalOperator<RowHolderFixed> input, ITransaction tran)
        {
            this.pageCollection = pageCollection;
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
