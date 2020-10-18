using MetadataManager;
using PageManager;
using System.Collections.Generic;

namespace QueryProcessing
{
    internal class PhyOpOrderBy : IPhysicalOperator<RowHolder>
    {
        private readonly IPhysicalOperator<RowHolder> source;
        private readonly OrderByColumn[] orderByColumns;

        public PhyOpOrderBy(IPhysicalOperator<RowHolder> source, OrderByColumn[] orderByColumns)
        {
            this.source = source;
            this.orderByColumns = orderByColumns;
        }

        public MetadataColumn[] GetOutputColumns()
        {
            return source.GetOutputColumns();
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            List<RowHolder> list = new List<RowHolder>();

            await foreach (RowHolder row in this.source.Iterate(tran))
            {
                list.Add(row);
            }

            list.Sort(new RowHolderOrderByComparer(this.orderByColumns));

            foreach (RowHolder row in list)
            {
                yield return row;
            }
        }
    }
}