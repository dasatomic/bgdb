using PageManager;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpProject : IPhysicalOperator<Row>
    {
        private IPhysicalOperator<Row> source;
        private int[] columnChooser;

        public PhyOpProject(IPhysicalOperator<Row> source, int[] columnChooser)
        {
            this.source = source;
            this.columnChooser = columnChooser;
        }

        public async IAsyncEnumerable<Row> Iterate(ITransaction tran)
        {
            await foreach (Row row in this.source.Iterate(tran))
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
