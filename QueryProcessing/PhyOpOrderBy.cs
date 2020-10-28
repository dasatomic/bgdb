using MetadataManager;
using PageManager;
using System.Collections.Generic;

namespace QueryProcessing
{
    public class PhyOpOrderBy : IPhysicalOperator<RowHolder>
    {
        private readonly IPhysicalOperator<RowHolder> source;
        private readonly IComparer<RowHolder> comparer;

        public PhyOpOrderBy(IPhysicalOperator<RowHolder> source, IComparer<RowHolder> comparer)
        {
            this.source = source;
            this.comparer = comparer;
        }

        public MetadataColumn[] GetOutputColumns()
        {
            return source.GetOutputColumns();
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            // TODO: Temporary solution with in-memory sort operation.
            //       This will have to change when we add spill operations.
            //
            List<RowHolder> list = new List<RowHolder>();

            await foreach (RowHolder row in this.source.Iterate(tran))
            {
                list.Add(row);
            }

            list.Sort(comparer);

            foreach (RowHolder row in list)
            {
                yield return row;
            }
        }
    }
}