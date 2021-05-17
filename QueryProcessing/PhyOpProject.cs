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
        bool isStar = false;
        int? topRows;

        public PhyOpProject(IPhysicalOperator<RowHolder> source, int[] columnChooser, int? topRows)
        {
            this.source = source;
            this.columnChooser = columnChooser;
            this.topRows = topRows;
            this.isStar = false;
        }

        public PhyOpProject(IPhysicalOperator<RowHolder> source, int? topRows)
        {
            this.source = source;
            this.topRows = topRows;
            this.isStar = true;
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            await foreach (RowHolder row in this.source.Iterate(tran))
            {
                if (this.isStar)
                {
                    yield return row;
                }
                else
                {
                    yield return row.Project(this.columnChooser);
                }

                if (--this.topRows == 0)
                {
                    yield break;
                }
            }
        }

        public async Task Invoke()
        {
            await Task.FromResult(0);
        }

        public MetadataColumn[] GetOutputColumns()
        {
            if (isStar)
            {
                return source.GetOutputColumns();
            }
            else
            {
                MetadataColumn[] sourceColumns = source.GetOutputColumns();
                return columnChooser.Select(cc => sourceColumns[cc]).ToArray();
            }
        }
    }
}
