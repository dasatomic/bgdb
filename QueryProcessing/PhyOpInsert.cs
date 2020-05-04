using MetadataManager;

namespace QueryProcessing
{
    public class PhyOpTableInsert : IPhysicalOperator
    {
        private MetadataTable mdTable;

        public PhyOpTableInsert(MetadataTable mdTable)
        {
            this.mdTable = mdTable;
        }
    }
}
