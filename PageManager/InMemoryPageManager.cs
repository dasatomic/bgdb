using System;
using System.Collections.Generic;
using System.Linq;

namespace PageManager
{
    public class InMemoryPageManager : IPageManager<IntegerOnlyPage>
    {
        private List<IPage> pages = new List<IPage>();
        private uint pageSize;
        private ulong lastUsedPageId = 0;

        public InMemoryPageManager(uint defaultPageSize)
        {
            this.pageSize = defaultPageSize;
        }

        public IPage AllocatePage(PageType pageType)
        {
            IPage page;
            ulong pageId = lastUsedPageId++;

            if (pageType == PageType.IntPage)
            {
                page = new IntegerOnlyPage(pageSize, pageId);
            }
            else
            {
                throw new ArgumentException("Unknown page type");
            }

            pages.Add(page);

            return page;
        }

        public IPage GetPage(ulong pageId)
        {
            IPage page = pages.FirstOrDefault(page => page.PageId() == pageId);

            if (page == null)
            {
                throw new PageNotFoundException();
            }

            return page;
        }

        public void SavePage(IPage page)
        {
            // No op. Should release the lock.
        }

        public IntegerOnlyPage AllocatePageSerializer()
        {
            IPage page = AllocatePage(PageType.IntPage);
            return (IntegerOnlyPage)page;
        }

        public IntegerOnlyPage GetPageSerializer(ulong pageId)
        {
            IPage page = this.GetPage(pageId);

            if (page.PageType() != PageType.IntPage)
            {
                throw new InvalidCastException("Can't cast to int page");
            }

            return (IntegerOnlyPage)page;
        }
    }
}
