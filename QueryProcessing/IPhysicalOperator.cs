using System.Collections.Generic;

namespace QueryProcessing
{
    public interface IPhysicalOperator<T> : IEnumerable<T>
    {
        void Invoke();
    }
}
