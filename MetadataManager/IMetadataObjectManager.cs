using PageManager;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MetadataManager
{
    public interface IMetadataObjectManager<O, C>
    {
        Task<int> CreateObject(C def, ITransaction tran);
        Task<bool> Exists(C def, ITransaction tran);
        Task<O> GetById(int id, ITransaction tran);
        IAsyncEnumerable<O> Iterate(ITransaction tran);
    }
}
