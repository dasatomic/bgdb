using PageManager;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpStaticRowProvider : IPhysicalOperator<RowHolderFixed>
    {
        private IEnumerable<RowHolderFixed> source;

        public PhyOpStaticRowProvider(IEnumerable<RowHolderFixed> rows)
        {
            this.source = rows;
        }

        public PhyOpStaticRowProvider(RowHolderFixed row)
        {
            this.source = new RowHolderFixed[] { row };
        }

        public async IAsyncEnumerable<RowHolderFixed> Iterate(ITransaction _)
        {
            foreach (RowHolderFixed row in source)
            {
                yield return await Task.FromResult(row);
            }
        }

        public async Task Invoke()
        {
            await Task.FromResult(0);
        }
    }
}
