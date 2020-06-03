using LogManager;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace PageManager
{
    public class InMemoryPageManager : IPageManager
    {
        private List<IPage> pages = new List<IPage>();
        private uint pageSize;
        private ulong lastUsedPageId = 1;
        private IPageEvictionPolicy pageEvictionPolicy;
        private IPersistedStream persistedStream;

        public InMemoryPageManager(uint defaultPageSize, IPageEvictionPolicy evictionPolicy, IPersistedStream persistedStream)
        {
            this.pageSize = defaultPageSize;
            this.pageEvictionPolicy = evictionPolicy;
            this.persistedStream = persistedStream;
        }

        public IPage AllocatePage(PageType pageType, ulong prevPageId, ulong nextPageId, ITransaction tran)
        {
            return AllocatePage(pageType, null, prevPageId, nextPageId, tran);
        }

        public IPage AllocatePage(PageType pageType, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId, ITransaction tran)
        {
            return AllocatePage(pageType, columnTypes, prevPageId, nextPageId, lastUsedPageId++, tran);
        }

        public IPage AllocatePageBootPage(PageType pageType, ColumnType[] columnTypes, ITransaction tran)
        {
            return AllocatePage(pageType, columnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, IBootPageAllocator.BootPageId, tran);
        }

        public IPage AllocatePage(PageType pageType, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId, ulong pageId, ITransaction tran)
        {
            IPage page;

            if (this.pages.Any(page => page.PageId() == pageId))
            {
                throw new PageCorruptedException();
            }

            if (pageType == PageType.IntPage)
            {
                page = new IntegerOnlyPage(pageSize, pageId, prevPageId, nextPageId, tran);
            }
            else if (pageType == PageType.DoublePage)
            {
                page = new DoubleOnlyPage(pageSize, pageId, prevPageId, nextPageId, tran);
            }
            else if (pageType == PageType.StringPage)
            {
                page = new StringOnlyPage(pageSize, pageId, prevPageId, nextPageId, tran);
            }
            else if (pageType == PageType.MixedPage)
            {
                page = new MixedPage(pageSize, pageId, columnTypes, prevPageId, nextPageId, tran);
            }
            else if (pageType == PageType.LongPage)
            {
                page = new LongOnlyPage(pageSize, pageId, prevPageId, nextPageId, tran);
            }
            else
            {
                throw new ArgumentException("Unknown page type");
            }

            pages.Add(page);

            if (prevPageId != PageManagerConstants.NullPageId)
            {
                IPage prevPage = GetPage(prevPageId, tran);

                if (prevPage.NextPageId() != page.NextPageId())
                {
                    throw new ArgumentException("Breaking the link");
                }

                prevPage.SetNextPageId(page.PageId());
            }

            if (nextPageId != PageManagerConstants.NullPageId)
            {
                IPage nextPage = GetPage(nextPageId, tran);

                if (nextPage.PrevPageId() != page.PrevPageId())
                {
                    throw new ArgumentException("breaking the link");
                }

                nextPage.SetPrevPageId(page.PageId());
            }

            foreach (ulong pageIdToEvict in pageEvictionPolicy.RecordUsageAndEvict(page.PageId()))
            {
                using (ITransaction evictTran = new ReadonlyTransaction())
                {
                    // TODO: All of this needs to be async.
                    IPage pageToEvict = this.GetPage(pageIdToEvict, tran);
                    this.FlushPage(pageToEvict);
                }
            }

            return page;
        }

        public IPage GetPage(ulong pageId, ITransaction tran)
        {
            IPage page = pages.FirstOrDefault(page => page.PageId() == pageId);

            if (page == null)
            {
                throw new PageNotFoundException();
            }

            return page;
        }

        internal void FlushPage(IPage page)
        {
            ulong position = page.PageId() * this.pageSize;
            this.persistedStream.SeekAndAccess(position, (stream) => page.Persist(stream));
        }

        public IntegerOnlyPage AllocatePageInt(ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = AllocatePage(PageType.IntPage, prevPage, nextPage, tran);
            return (IntegerOnlyPage)page;
        }

        public IntegerOnlyPage GetPageInt(ulong pageId, ITransaction tran)
        {
            IPage page = this.GetPage(pageId, tran);

            if (page.PageType() != PageType.IntPage)
            {
                throw new InvalidCastException("Can't cast to int page");
            }

            return (IntegerOnlyPage)page;
        }

        public DoubleOnlyPage AllocatePageDouble(ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = AllocatePage(PageType.DoublePage, prevPage, nextPage, tran);
            return (DoubleOnlyPage)page;
        }

        public DoubleOnlyPage GetPageDouble(ulong pageId, ITransaction tran)
        {
            IPage page = this.GetPage(pageId, tran);

            if (page.PageType() != PageType.DoublePage)
            {
                throw new InvalidCastException("Can't cast to double page");
            }

            return (DoubleOnlyPage)page;
        }

        public StringOnlyPage AllocatePageStr(ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = AllocatePage(PageType.StringPage, prevPage, nextPage, tran);
            return (StringOnlyPage)page;
        }

        public StringOnlyPage GetPageStr(ulong pageId, ITransaction tran)
        {
            IPage page = this.GetPage(pageId, tran);

            if (page.PageType() != PageType.StringPage)
            {
                throw new InvalidCastException("Can't cast to string page");
            }

            return (StringOnlyPage)page;
        }

        public LongOnlyPage AllocatePageLong(ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = AllocatePage(PageType.LongPage, prevPage, nextPage, tran);
            return (LongOnlyPage)page;
        }

        public LongOnlyPage GetPageLong(ulong pageId, ITransaction tran)
        {
            IPage page = this.GetPage(pageId, tran);

            if (page.PageType() != PageType.LongPage)
            {
                throw new InvalidCastException("Can't cast to long page");
            }

            return (LongOnlyPage)page;
        }

        public MixedPage AllocateMixedPage(ColumnType[] columnTypes, ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = AllocatePage(PageType.MixedPage, columnTypes, prevPage, nextPage, tran);
            return (MixedPage)page;
        }

        public MixedPage GetMixedPage(ulong pageId, ITransaction tran)
        {
            IPage page = this.GetPage(pageId, tran);

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

        public ulong PageCount() => (ulong)this.pages.Count;
    }
}
