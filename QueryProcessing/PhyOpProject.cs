using PageManager;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpProject : IPhysicalOperator<RowHolderFixed>
    {
        private IPhysicalOperator<RowHolderFixed> source;
        private int[] columnChooser;

        public PhyOpProject(IPhysicalOperator<RowHolderFixed> source, int[] columnChooser)
        {
            this.source = source;
            this.columnChooser = columnChooser;
        }

        public async IAsyncEnumerable<RowHolderFixed> Iterate(ITransaction tran)
        {
            await foreach (RowHolderFixed row in this.source.Iterate(tran))
            {
                yield return row.Project(this.columnChooser);
            }
        }

        public async Task Invoke()
        {
            await Task.FromResult(0);
        }
    }
}
