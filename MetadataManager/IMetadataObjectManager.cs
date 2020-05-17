using PageManager;
using System.Collections.Generic;

namespace MetadataManager
{
    public interface IMetadataObjectManager<O, C>
    {
        int CreateObject(C def, ITransaction tran);
        bool Exists(C def, ITransaction tran);
        O GetById(int id, ITransaction tran);
        IEnumerable<O> Iterate(ITransaction tran);
    }
}
