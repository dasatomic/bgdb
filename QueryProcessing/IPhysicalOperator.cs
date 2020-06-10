using PageManager;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public interface IPhysicalOperator<T>
    {
        Task Invoke();
        IAsyncEnumerable<T> Iterate(ITransaction tran);
    }
}
