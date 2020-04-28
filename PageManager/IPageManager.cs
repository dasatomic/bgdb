using System.Collections.Generic;

namespace PageManager
{
    public interface IPageManager
    {
        IPage GetPage(long pageId);

        void SavePage(IPage page);

        IEnumerable<IPage> GetPages();

        IPage AllocatePage();
    }
}
