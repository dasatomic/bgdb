using MetadataManager;
using PageManager;
using System.Linq;

namespace QueryProcessing
{
    public class PhyOpTableInsert : IPhysicalOperator
    {
        private MetadataTable mdTable;
        private PageListCollection pageCollection;
        private IAllocateMixedPage pageAllocator;

        public PhyOpTableInsert(MetadataTable mdTable, IAllocateMixedPage pageAllocator)
        {
            this.mdTable = mdTable;
            this.pageAllocator = pageAllocator;

            IPage rootPage = this.pageAllocator.GetMixedPage(this.mdTable.RootPage);
            this.pageCollection = new PageListCollection(this.pageAllocator, mdTable.Columns.Select(c => c.ColumnType).ToArray(), rootPage);
        }
    }
}
