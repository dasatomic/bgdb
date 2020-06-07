using LogManager;
using System;
using System.Collections.Generic;

namespace PageManager
{
    public class PageManager : IPageManager
    {
        private List<ulong> pageIds = new List<ulong>();
        private uint pageSize;
        private ulong lastUsedPageId = 1;
        private IPageEvictionPolicy pageEvictionPolicy;
        private IPersistedStream persistedStream;
        private IBufferPool bufferPool;

        public PageManager(uint defaultPageSize, IPageEvictionPolicy evictionPolicy, IPersistedStream persistedStream)
        {
            this.pageSize = defaultPageSize;
            this.pageEvictionPolicy = evictionPolicy;
            this.persistedStream = persistedStream;

            this.bufferPool = new BufferPool();
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

            if (this.pageIds.Contains(pageId))
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

            this.pageIds.Add(pageId);

            if (prevPageId != PageManagerConstants.NullPageId)
            {
                IPage prevPage = GetPage(prevPageId, tran, pageType, columnTypes);

                if (prevPage.NextPageId() != page.NextPageId())
                {
                    throw new ArgumentException("Breaking the link");
                }

                prevPage.SetNextPageId(page.PageId());
            }

            if (nextPageId != PageManagerConstants.NullPageId)
            {
                IPage nextPage = GetPage(nextPageId, tran, pageType, columnTypes);

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
                    IPage pageToEvict = this.bufferPool.GetPage(pageIdToEvict);
                    this.FlushPage(pageToEvict);

                    bufferPool.EvictPage(pageToEvict.PageId());
                }
            }

            bufferPool.AddPage(page);

            return page;
        }

        public IPage GetPage(ulong pageId, ITransaction tran, PageType pageType, ColumnType[] columnTypes)
        {
            if (!this.pageIds.Contains(pageId))
            {
                throw new PageNotFoundException();
            }

            IPage page = this.bufferPool.GetPage(pageId);

            if (page == null)
            {
                page = this.FetchPage(pageId, pageType, columnTypes);
                this.bufferPool.AddPage(page);
            }

            foreach (ulong pageIdToEvict in this.pageEvictionPolicy.RecordUsageAndEvict(pageId))
            {
                IPage pageToEvict = this.bufferPool.GetPage(pageIdToEvict);
                this.FlushPage(pageToEvict);
                this.bufferPool.EvictPage(pageIdToEvict);
            }

            return page;
        }

        internal void FlushPage(IPage page)
        {
            ulong position = page.PageId() * this.pageSize;
            this.persistedStream.SeekAndWrite(position, (stream) => page.Persist(stream));
        }

        internal IPage FetchPage(ulong pageId, PageType pageType, ColumnType[] columnTypes)
        {
            ulong position = pageId * this.pageSize;
            return this.persistedStream.SeekAndRead(position, (stream) =>
            {
                if (pageType == PageType.DoublePage)
                {
                    return new DoubleOnlyPage(stream);
                }
                else if (pageType == PageType.IntPage)
                {
                    return new IntegerOnlyPage(stream);
                }
                else if (pageType == PageType.LongPage)
                {
                    return new LongOnlyPage(stream);
                }
                else if (pageType == PageType.MixedPage)
                {
                    return new MixedPage(stream, columnTypes);
                }
                else if (pageType == PageType.StringPage)
                {
                    return new StringOnlyPage(stream);
                }
                else
                {
                    throw new ArgumentException();
                }
            });
        }

        public IntegerOnlyPage AllocatePageInt(ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = AllocatePage(PageType.IntPage, prevPage, nextPage, tran);
            return (IntegerOnlyPage)page;
        }

        public IntegerOnlyPage GetPageInt(ulong pageId, ITransaction tran)
        {
            IPage page = this.GetPage(pageId, tran, PageType.IntPage, null);

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
            IPage page = this.GetPage(pageId, tran, PageType.DoublePage, null);

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
            IPage page = this.GetPage(pageId, tran, PageType.StringPage, null);

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
            IPage page = this.GetPage(pageId, tran, PageType.LongPage, null);

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

        public MixedPage GetMixedPage(ulong pageId, ITransaction tran, ColumnType[] columnTypes)
        {
            IPage page = this.GetPage(pageId, tran, PageType.MixedPage, columnTypes);

            if (page.PageType() != PageType.MixedPage)
            {
                throw new InvalidCastException("Can't cast to mixed page");
            }

            return (MixedPage)page;
        }

        public bool BootPageInitialized()
        {
            // return pages.Any(p => p.PageId() == IBootPageAllocator.BootPageId);
            return this.bufferPool.GetPage(IBootPageAllocator.BootPageId) != null;
        }

        public ulong PageCount() => (ulong)this.pageIds.Count;
    }
}
