using PageManager;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpFilter : IPhysicalOperator<RowHolder>
    {
        private Func<RowHolder, bool> filterFunc;
        private IPhysicalOperator<RowHolder> source;

        public PhyOpFilter(IPhysicalOperator<RowHolder> source, Func<RowHolder, bool> filterFunc)
        {
            this.source = source;
            this.filterFunc = filterFunc;
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            // TODO: Filter is currently applied externally, in query processing layer.
            // It would be much more efficient if filter could be pushed to storage layer
            // and have RowsetHolder return only subset of rows based on filter.

            await foreach (RowHolder row in this.source.Iterate(tran))
            {
                if (filterFunc(row))
                {
                    yield return row;
                }
            }
        }

        public async Task Invoke()
        {
            await Task.FromResult(0);
        }
    }
}
