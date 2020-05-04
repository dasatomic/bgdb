using PageManager;
using System.Collections.Generic;

namespace MetadataManager
{
    public interface IMetadataObjectManager<O>
    {
        IEnumerator<O> GetEnumerator();
    }
}
