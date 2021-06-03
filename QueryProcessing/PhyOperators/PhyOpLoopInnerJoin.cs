using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;

namespace QueryProcessing
{
    public class PhyOpLoopInnerJoin : IPhysicalOperator<RowHolder>
    {
        private readonly Func<RowHolder, bool> filterFunc;
        private readonly IPhysicalOperator<RowHolder> sourceLeft;
        private readonly IPhysicalOperator<RowHolder> sourceRight;
        private readonly MetadataColumn[] returnMdColumns;

        public PhyOpLoopInnerJoin(IPhysicalOperator<RowHolder> sourceLeft, IPhysicalOperator<RowHolder> sourceRight, Func<RowHolder, bool> filterFunc)
        {
            this.filterFunc = filterFunc;
            this.sourceLeft = sourceLeft;
            this.sourceRight = sourceRight;

            this.returnMdColumns = QueryProcessingAccessors.MergeColumns(sourceLeft.GetOutputColumns(), sourceRight.GetOutputColumns());
        }

        public MetadataColumn[] GetOutputColumns() => returnMdColumns;

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            await foreach (RowHolder rowLeft in this.sourceLeft.Iterate(tran))
            {
                await foreach (RowHolder rowRight in this.sourceRight.Iterate(tran))
                {
                    // merge and apply filter.
                    // TODO: this, of course, is not optimal, to say the least...
                    RowHolder row = rowLeft.Merge(rowRight);

                    if (filterFunc(row))
                    {
                        yield return row;
                    }
                }
            }
        }
    }
}
