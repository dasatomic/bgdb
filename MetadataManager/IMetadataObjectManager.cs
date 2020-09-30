using PageManager;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MetadataManager
{
    public interface IMetadataObjectManager<O /* Metadata object */, C /* Create definition */, U /* Unique constraint tuple */>
    {
        Task<int> CreateObject(C def, ITransaction tran);
        Task<bool> Exists(C def, ITransaction tran);
        Task<O> GetById(U id, ITransaction tran);
        IAsyncEnumerable<O> Iterate(ITransaction tran);
    }
}
