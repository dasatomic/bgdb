using MetadataManager;
using PageManager;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpProject : IPhysicalOperator<RowHolder>
    {
        private IPhysicalOperator<RowHolder> source;
        private int[] columnChooser;

        public PhyOpProject(IPhysicalOperator<RowHolder> source, int[] columnChooser)
        {
            this.source = source;
            this.columnChooser = columnChooser;
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            await foreach (RowHolder row in this.source.Iterate(tran))
            {
                yield return row.Project(this.columnChooser);
            }
        }

        public async Task Invoke()
        {
            await Task.FromResult(0);
        }

        public MetadataColumn[] GetOutputColumns()
        {
            MetadataColumn[] sourceColumns = source.GetOutputColumns();
            return columnChooser.Select(cc => sourceColumns[cc]).ToArray();
        }
    }
}
