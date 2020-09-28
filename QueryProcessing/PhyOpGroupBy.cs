using PageManager;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpGroupBy : IPhysicalOperator<RowHolderFixed>
    {
        private GroupByFunctors functors;
        private IPhysicalOperator<RowHolderFixed> source;

        public PhyOpGroupBy(IPhysicalOperator<RowHolderFixed> source, GroupByFunctors functors)
        {
            this.source = source;
            this.functors = functors;
        }

        public async IAsyncEnumerable<RowHolderFixed> Iterate(ITransaction tran)
        {
            // TODO: Group by can work with disk spills. This comes later when we get support for tempdb and spills.

            // Grouped part -> entire row with latest state.
            Dictionary<RowHolderFixed, RowHolderFixed> groupSet = new Dictionary<RowHolderFixed, RowHolderFixed>();

            await foreach (RowHolderFixed row in this.source.Iterate(tran))
            {
                // This is key for grouper.
                RowHolderFixed groupedPart = functors.Grouper(row);
                RowHolderFixed projectPart = functors.Projector(row);

                if (groupSet.TryGetValue(groupedPart, out RowHolderFixed rowFromGroup /* this is the state */))
                {
                    RowHolderFixed newState = functors.Aggregate(projectPart, rowFromGroup);
                    groupSet[groupedPart] = newState;
                }
                else
                {
                    groupSet[groupedPart] = projectPart;
                }
            }

            foreach (RowHolderFixed rhf in groupSet.Values)
            {
                yield return rhf;
            }
        }

        public async Task Invoke()
        {
            await Task.FromResult(0);
        }

    }
}
