using PageManager;
using System.Collections.Generic;

namespace MetadataManager
{
    public interface IMetadataObjectManager<O, C> : IEnumerable<O>
    {
        int CreateObject(C def);
        bool Exists(C def);
        O GetById(int id);
    }
}
