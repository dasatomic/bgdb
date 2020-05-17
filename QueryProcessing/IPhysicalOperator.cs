using PageManager;
using System.Collections.Generic;

namespace QueryProcessing
{
    public interface IPhysicalOperator<T>
    {
        void Invoke();
        IEnumerable<T> Iterate(ITransaction tran);
    }
}
