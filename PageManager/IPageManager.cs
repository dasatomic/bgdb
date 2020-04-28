using System.Collections.Generic;

namespace PageManager
{
    public interface IPageManager<T>
    {
        IPage GetPage(ulong pageId);
        void SavePage(IPage page);
        IPage AllocatePage(PageType pageType);
        T AllocatePageSerializer<T>();
        T GetPageSerializer<T>(ulong pageId);
    }
}
