using MetadataManager;
using PageManager;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpGroupBy : IPhysicalOperator<RowHolder>
    {
        private GroupByFunctors functors;
        private IPhysicalOperator<RowHolder> source;

        public PhyOpGroupBy(IPhysicalOperator<RowHolder> source, GroupByFunctors functors)
        {
            this.source = source;
            this.functors = functors;
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            // TODO: Group by can work with disk spills. This comes later when we get support for tempdb and spills.

            // Grouped part -> entire row with latest state.
            Dictionary<RowHolder, RowHolder> groupSet = new Dictionary<RowHolder, RowHolder>();

            await foreach (RowHolder row in this.source.Iterate(tran))
            {
                // This is key for grouper.
                RowHolder groupedPart = functors.Grouper(row);
                RowHolder projectPart = functors.Projector(row);

                if (groupSet.TryGetValue(groupedPart, out RowHolder rowFromGroup /* this is the state */))
                {
                    RowHolder newState = functors.Aggregate(projectPart, rowFromGroup);
                    groupSet[groupedPart] = newState;
                }
                else
                {
                    groupSet[groupedPart] = projectPart;
                }
            }

            foreach (RowHolder rhf in groupSet.Values)
            {
                yield return rhf;
            }
        }

        public async Task Invoke()
        {
            await Task.FromResult(0);
        }

        public MetadataColumn[] GetOutputColumns() => functors.ProjectColumnInfo;
    }
}
