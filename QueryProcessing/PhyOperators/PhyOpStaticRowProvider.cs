using MetadataManager;
using PageManager;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpStaticRowProvider : IPhysicalOperator<RowHolder>
    {
        private IEnumerable<RowHolder> source;

        public PhyOpStaticRowProvider(IEnumerable<RowHolder> rows)
        {
            this.source = rows;
        }

        public PhyOpStaticRowProvider(RowHolder row)
        {
            this.source = new RowHolder[] { row };
        }

        public MetadataColumn[] GetOutputColumns() => new MetadataColumn[0];

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction _)
        {
            foreach (RowHolder row in source)
            {
                yield return await Task.FromResult(row);
            }
        }
    }
}
