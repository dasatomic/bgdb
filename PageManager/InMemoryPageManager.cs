using System;
using System.Collections.Generic;
using System.Linq;

namespace PageManager
{
    public interface IBootPageAllocator
    {
        IPage AllocatePageBootPage(PageType pageType, ColumnType[] columnTypes);
        bool BootPageInitialized();

        public const ulong BootPageId = ulong.MaxValue;
    }

    public class InMemoryPageManager : IAllocateIntegerPage, IAllocateDoublePage, IAllocateStringPage, IAllocateLongPage, IAllocateMixedPage, IBootPageAllocator
    {
        private List<IPage> pages = new List<IPage>();
        private uint pageSize;
        private ulong lastUsedPageId = 1;

        public InMemoryPageManager(uint defaultPageSize)
        {
            this.pageSize = defaultPageSize;
        }

        public IPage AllocatePage(PageType pageType, ulong prevPageId, ulong nextPageId)
        {
            return AllocatePage(pageType, null, prevPageId, nextPageId);
        }

        public IPage AllocatePage(PageType pageType, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId)
        {
            return AllocatePage(pageType, columnTypes, prevPageId, nextPageId, lastUsedPageId++);
        }

        public IPage AllocatePageBootPage(PageType pageType, ColumnType[] columnTypes)
        {
            return AllocatePage(pageType, columnTypes, 0, 0, IBootPageAllocator.BootPageId);
        }

        private IPage AllocatePage(PageType pageType, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId, ulong pageId)
        {
            IPage page;

            if (pageType == PageType.IntPage)
            {
                page = new IntegerOnlyPage(pageSize, pageId, prevPageId, nextPageId);
            }
            else if (pageType == PageType.DoublePage)
            {
                page = new DoubleOnlyPage(pageSize, pageId, prevPageId, nextPageId);
            }
            else if (pageType == PageType.StringPage)
            {
                page = new StringOnlyPage(pageSize, pageId, prevPageId, nextPageId);
            }
            else if (pageType == PageType.MixedPage)
            {
                page = new MixedPage(pageSize, pageId, columnTypes, prevPageId, nextPageId);
            }
            else if (pageType == PageType.LongPage)
            {
                page = new LongOnlyPage(pageSize, pageId, prevPageId, nextPageId);
            }
            else
            {
                throw new ArgumentException("Unknown page type");
            }

            pages.Add(page);

            if (prevPageId != 0)
            {
                IPage prevPage = GetPage(prevPageId);

                if (prevPage.NextPageId() != page.NextPageId())
                {
                    throw new ArgumentException("Breaking the link");
                }

                prevPage.SetNextPageId(page.PageId());
            }

            if (nextPageId != 0)
            {
                IPage nextPage = GetPage(nextPageId);

                if (nextPage.PrevPageId() != page.PrevPageId())
                {
                    throw new ArgumentException("breaking the link");
                }

                nextPage.SetPrevPageId(page.PageId());
            }

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

        public IntegerOnlyPage AllocatePageInt(ulong prevPage, ulong nextPage)
        {
            IPage page = AllocatePage(PageType.IntPage, prevPage, nextPage);
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

        public DoubleOnlyPage AllocatePageDouble(ulong prevPage, ulong nextPage)
        {
            IPage page = AllocatePage(PageType.DoublePage, prevPage, nextPage);
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

        public StringOnlyPage AllocatePageStr(ulong prevPage, ulong nextPage)
        {
            IPage page = AllocatePage(PageType.StringPage, prevPage, nextPage);
            return (StringOnlyPage)page;
        }

        public StringOnlyPage GetPageStr(ulong pageId)
        {
            IPage page = this.GetPage(pageId);

            if (page.PageType() != PageType.StringPage)
            {
                throw new InvalidCastException("Can't cast to string page");
            }

            return (StringOnlyPage)page;
        }

        public LongOnlyPage AllocatePageLong(ulong prevPage, ulong nextPage)
        {
            IPage page = AllocatePage(PageType.LongPage, prevPage, nextPage);
            return (LongOnlyPage)page;
        }

        public LongOnlyPage GetPageLong(ulong pageId)
        {
            IPage page = this.GetPage(pageId);

            if (page.PageType() != PageType.LongPage)
            {
                throw new InvalidCastException("Can't cast to long page");
            }

            return (LongOnlyPage)page;
        }

        public MixedPage AllocateMixedPage(ColumnType[] columnTypes, ulong prevPage, ulong nextPage)
        {
            IPage page = AllocatePage(PageType.MixedPage, columnTypes, prevPage, nextPage);
            return (MixedPage)page;
        }

        public MixedPage GetMixedPage(ulong pageId)
        {
            IPage page = this.GetPage(pageId);

            if (page.PageType() != PageType.MixedPage)
            {
                throw new InvalidCastException("Can't cast to mixed page");
            }

            return (MixedPage)page;
        }

        public bool BootPageInitialized()
        {
            return pages.Any(p => p.PageId() == IBootPageAllocator.BootPageId);
        }
    }
}
