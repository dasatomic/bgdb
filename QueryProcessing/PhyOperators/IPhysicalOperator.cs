using MetadataManager;
using PageManager;
using System.Collections.Generic;

namespace QueryProcessing
{
    public interface IPhysicalOperator<T>
    {
        IAsyncEnumerable<T> Iterate(ITransaction tran);
        MetadataColumn[] GetOutputColumns();
    }
}
