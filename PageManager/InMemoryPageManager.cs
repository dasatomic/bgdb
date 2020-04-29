using System;
using System.Collections.Generic;
using System.Linq;

namespace PageManager
{
    public class InMemoryPageManager : IAllocateIntegerPage, IAllocateDoublePage, IAllocateStringPage, IAllocateLongPage, IAllocateMixedPage
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
            return AllocatePage(pageType, null);
        }

        public IPage AllocatePage(PageType pageType, ColumnType[] columnTypes)
        {
            IPage page;
            ulong pageId = lastUsedPageId++;

            if (pageType == PageType.IntPage)
            {
                page = new IntegerOnlyPage(pageSize, pageId);
            }
            else if (pageType == PageType.DoublePage)
            {
                page = new DoubleOnlyPage(pageSize, pageId);
            }
            else if (pageType == PageType.StringPage)
            {
                page = new StringOnlyPage(pageSize, pageId);
            }
            else if (pageType == PageType.MixedPage)
            {
                page = new MixedPage(pageSize, pageId, columnTypes);
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

        public IntegerOnlyPage AllocatePageInt()
        {
            IPage page = AllocatePage(PageType.IntPage);
            return (IntegerOnlyPage)page;
        }

        public IntegerOnlyPage GetPageInt(ulong pageId)
        {
            IPage page = this.GetPage(pageId);

            if (page.PageType() != PageType.IntPage)
            {
                throw new InvalidCastException("Can't cast to int page");
            }

            return (IntegerOnlyPage)page;
        }

        public DoubleOnlyPage AllocatePageDouble()
        {
            IPage page = AllocatePage(PageType.DoublePage);
            return (DoubleOnlyPage)page;
        }

        public DoubleOnlyPage GetPageDouble(ulong pageId)
        {
            IPage page = this.GetPage(pageId);

            if (page.PageType() != PageType.DoublePage)
            {
                throw new InvalidCastException("Can't cast to double page");
            }

            return (DoubleOnlyPage)page;
        }

        public StringOnlyPage AllocatePageStr()
        {
            IPage page = AllocatePage(PageType.StringPage);
            return (StringOnlyPage)page;
        }

        public StringOnlyPage GetPageStr(ulong pageId)
        {
            IPage page = this.GetPage(pageId);

            if (page.PageType() != PageType.StringPage)
            {
                throw new InvalidCastException("Can't cast to double page");
            }

            return (StringOnlyPage)page;
        }

        public LongOnlyPage AllocatePageLong()
        {
            IPage page = AllocatePage(PageType.LongPage);
            return (LongOnlyPage)page;
        }

        public LongOnlyPage GetPageLong(ulong pageId)
        {
            IPage page = this.GetPage(pageId);

            if (page.PageType() != PageType.LongPage)
            {
                throw new InvalidCastException("Can't cast to double page");
            }

            return (LongOnlyPage)page;
        }

        public MixedPage AllocateMixedPage(ColumnType[] columnTypes)
        {
            IPage page = AllocatePage(PageType.MixedPage, columnTypes);
            return (MixedPage)page;
        }

        public MixedPage GetMixedPage(ulong pageId)
        {
            IPage page = this.GetPage(pageId);

            if (page.PageType() != PageType.MixedPage)
            {
                throw new InvalidCastException("Can't cast to double page");
            }

            return (MixedPage)page;
        }
    }
}
