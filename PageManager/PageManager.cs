using LockManager;
using LogManager;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PageManager
{
    public class PageManager : IPageManager
    {
        private ConcurrentBag<ulong> pageIds = new ConcurrentBag<ulong>();
        private uint pageSize;
        private ulong lastUsedPageId = 2;
        private readonly IPageEvictionPolicy pageEvictionPolicy;
        private readonly IPersistedStream persistedStream;
        private readonly IBufferPool bufferPool;

        private const int AllocationMapPageId = 1;
        private readonly List<IntegerOnlyPage> AllocatationMapPages;
        private readonly ILockManager lockManager;
        private SemaphoreSlim globalSemaphore = new SemaphoreSlim(1, 1);

        public PageManager(uint defaultPageSize, IPageEvictionPolicy evictionPolicy, IPersistedStream persistedStream)
            : this(defaultPageSize, evictionPolicy, persistedStream, new BufferPool(), new LockManager.LockManager())
        {
        }

        public PageManager(uint defaultPageSize, IPageEvictionPolicy evictionPolicy, IPersistedStream persistedStream, IBufferPool bufferPool, ILockManager lockManager)
        {
            this.pageSize = defaultPageSize;
            this.pageEvictionPolicy = evictionPolicy;
            this.persistedStream = persistedStream;
            this.lockManager = lockManager;

            this.bufferPool = bufferPool;

            this.AllocatationMapPages = new List<IntegerOnlyPage>();

            if (!this.persistedStream.IsInitialized())
            {
                using (ITransaction tran = new NotLoggedTransaction())
                {
                    IntegerOnlyPage allocationMapFirstPage = allocationMapFirstPage = new IntegerOnlyPage(pageSize, (ulong)AllocationMapPageId, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
                    this.AllocatationMapPages.Add(allocationMapFirstPage);
                }
            }
            else
            {
                // TODO: Read boot page.
                ulong position = AllocationMapPageId * this.pageSize;
                IntegerOnlyPage allocationMapFirstPage = (IntegerOnlyPage)this.persistedStream.SeekAndRead(position, PageType.IntPage, null).Result;
                this.AllocatationMapPages.Add(allocationMapFirstPage);

                ulong elemPosInPage = 0;
                using (ITransaction tran = new NotLoggedTransaction())
                {
                    foreach (int elem in allocationMapFirstPage.Fetch(tran))
                    {
                        for (int i = 0; i < 32; i++)
                        {
                            if ((elem & (0x1 << i)) != 0)
                            {
                                this.pageIds.Add(elemPosInPage * 32UL + (ulong)i);
                            }
                        }

                        elemPosInPage++;
                    }
                }
            }
        }

        public async Task<IPage> AllocatePage(PageType pageType, ulong prevPageId, ulong nextPageId, ITransaction tran)
        {
            return await AllocatePage(pageType, null, prevPageId, nextPageId, tran);
        }

        public async Task<IPage> AllocatePage(PageType pageType, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId, ITransaction tran)
        {
            return await AllocatePage(pageType, columnTypes, prevPageId, nextPageId, lastUsedPageId++, tran);
        }

        public async Task<IPage> AllocatePageBootPage(PageType pageType, ColumnType[] columnTypes, ITransaction tran)
        {
            return await AllocatePage(pageType, columnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, IBootPageAllocator.BootPageId, tran);
        }

        public async Task<IPage> AllocatePage(PageType pageType, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId, ulong pageId, ITransaction tran)
        {
            IPage page;

            if (this.pageIds.Contains(pageId))
            {
                throw new PageCorruptedException();
            }

            page = pageType switch
            {
                PageType.IntPage => new IntegerOnlyPage(pageSize, pageId, prevPageId, nextPageId, tran),
                PageType.DoublePage => new DoubleOnlyPage(pageSize, pageId, prevPageId, nextPageId, tran),
                PageType.StringPage => new StringOnlyPage(pageSize, pageId, prevPageId, nextPageId, tran),
                PageType.MixedPage => new MixedPage(pageSize, pageId, columnTypes, prevPageId, nextPageId, tran),
                PageType.LongPage => new LongOnlyPage(pageSize, pageId, prevPageId, nextPageId, tran),
                _ => throw new ArgumentException("Unknown page type")
            };

            this.pageIds.Add(pageId);

            if (prevPageId != PageManagerConstants.NullPageId)
            {
                tran.VerifyLock(prevPageId, LockTypeEnum.Exclusive);
                IPage prevPage = await GetPage(prevPageId, tran, pageType, columnTypes);

                if (prevPage.NextPageId() != page.NextPageId())
                {
                    throw new ArgumentException("Breaking the link");
                }

                prevPage.SetNextPageId(page.PageId());
            }

            if (nextPageId != PageManagerConstants.NullPageId)
            {
                IPage nextPage = await GetPage(nextPageId, tran, pageType, columnTypes);

                if (nextPage.PrevPageId() != page.PrevPageId())
                {
                    throw new ArgumentException("breaking the link");
                }

                nextPage.SetPrevPageId(page.PageId());
            }

            await globalSemaphore.WaitAsync();

            // TODO: Need to check all the locks here.
            foreach (ulong pageIdToEvict in pageEvictionPolicy.RecordUsageAndEvict(page.PageId()))
            {
                IPage pageToEvict = this.bufferPool.GetPage(pageIdToEvict);
                await this.FlushPage(pageToEvict);

                bufferPool.EvictPage(pageToEvict.PageId());
            }

            bufferPool.AddPage(page);

            AddToAllocationMap(page);

            globalSemaphore.Release();

            return page;
        }

        private void AddToAllocationMap(IPage page)
        {
            IntegerOnlyPage AMPage = this.AllocatationMapPages.Last();
            uint maxFit = AMPage.MaxRowCount() * sizeof(int) * 8;
            uint allocationMapPage = (uint)(page.PageId() / maxFit);

            if (allocationMapPage == this.AllocatationMapPages.Count)
            {
                using (ITransaction gamAllocTran = new NotLoggedTransaction())
                {
                    IntegerOnlyPage newAmPage = new IntegerOnlyPage(pageSize, (ulong)AllocationMapPageId + maxFit, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, gamAllocTran);
                    this.AllocatationMapPages.Add(newAmPage);
                }
            }

            using (ITransaction gamUpdateTran = new NotLoggedTransaction())
            {
                IntegerOnlyPage gamPage = this.AllocatationMapPages.ElementAt((int)allocationMapPage);
                int positionInPage = (int)(page.PageId() % maxFit);

                if (gamPage.RowCount() * sizeof(int) * 8 > positionInPage)
                {
                    int[] elems = gamPage.Fetch(gamUpdateTran);

                    int elemPos = positionInPage / (8 * sizeof(int));
                    int offset = positionInPage % (8 * sizeof(int));
                    elems[elemPos] |= 0x1 << offset;

                    gamPage.Update(new int[1] { elems[elemPos] }, (ushort)elemPos, gamUpdateTran);
                }
                else
                {
                    gamPage.Merge(new int[1] { 1 << positionInPage }, gamUpdateTran);
                }
            }
        }

        public async Task<IPage> GetPage(ulong pageId, ITransaction tran, PageType pageType, ColumnType[] columnTypes)
        {
            tran.VerifyLock(pageId, LockTypeEnum.Shared);

            if (!this.pageIds.Contains(pageId))
            {
                throw new PageNotFoundException();
            }

            await this.globalSemaphore.WaitAsync();
            IPage page = this.bufferPool.GetPage(pageId);

            if (page == null)
            {
                page = await this.FetchPage(pageId, pageType, columnTypes);
                this.bufferPool.AddPage(page);
            }

            foreach (ulong pageIdToEvict in this.pageEvictionPolicy.RecordUsageAndEvict(pageId))
            {
                IPage pageToEvict = this.bufferPool.GetPage(pageIdToEvict);
                await this.FlushPage(pageToEvict);
                this.bufferPool.EvictPage(pageIdToEvict);
            }

            this.globalSemaphore.Release();

            return page;
        }

        internal async Task FlushPage(IPage page)
        {
            if (page.IsDirty())
            {
                ulong position = page.PageId() * this.pageSize;
                await this.persistedStream.SeekAndWrite(position, page);
                page.ResetDirty();
            }
        }

        internal async Task<IPage> FetchPage(ulong pageId, PageType pageType, ColumnType[] columnTypes)
        {
            ulong position = pageId * this.pageSize;
            return await this.persistedStream.SeekAndRead(position, pageType, columnTypes);
        }

        public async Task<IntegerOnlyPage> AllocatePageInt(ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = await AllocatePage(PageType.IntPage, prevPage, nextPage, tran);
            return (IntegerOnlyPage)page;
        }

        public async Task<IntegerOnlyPage> GetPageInt(ulong pageId, ITransaction tran)
        {
            IPage page = await this.GetPage(pageId, tran, PageType.IntPage, null);

            if (page.PageType() != PageType.IntPage)
            {
                throw new InvalidCastException("Can't cast to int page");
            }

            return (IntegerOnlyPage)page;
        }

        public async Task<DoubleOnlyPage> AllocatePageDouble(ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = await AllocatePage(PageType.DoublePage, prevPage, nextPage, tran);
            return (DoubleOnlyPage)page;
        }

        public async Task<DoubleOnlyPage> GetPageDouble(ulong pageId, ITransaction tran)
        {
            IPage page = await this.GetPage(pageId, tran, PageType.DoublePage, null);

            if (page.PageType() != PageType.DoublePage)
            {
                throw new InvalidCastException("Can't cast to double page");
            }

            return (DoubleOnlyPage)page;
        }

        public async Task<StringOnlyPage> AllocatePageStr(ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = await AllocatePage(PageType.StringPage, prevPage, nextPage, tran);
            return (StringOnlyPage)page;
        }

        public async Task<StringOnlyPage> GetPageStr(ulong pageId, ITransaction tran)
        {
            IPage page = await this.GetPage(pageId, tran, PageType.StringPage, null);

            if (page.PageType() != PageType.StringPage)
            {
                throw new InvalidCastException("Can't cast to string page");
            }

            return (StringOnlyPage)page;
        }

        public async Task<LongOnlyPage> AllocatePageLong(ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = await AllocatePage(PageType.LongPage, prevPage, nextPage, tran);
            return (LongOnlyPage)page;
        }

        public async Task<LongOnlyPage> GetPageLong(ulong pageId, ITransaction tran)
        {
            IPage page = await this.GetPage(pageId, tran, PageType.LongPage, null);

            if (page.PageType() != PageType.LongPage)
            {
                throw new InvalidCastException("Can't cast to long page");
            }

            return (LongOnlyPage)page;
        }

        public async Task<MixedPage> AllocateMixedPage(ColumnType[] columnTypes, ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = await AllocatePage(PageType.MixedPage, columnTypes, prevPage, nextPage, tran);
            return (MixedPage)page;
        }

        public async Task<MixedPage> GetMixedPage(ulong pageId, ITransaction tran, ColumnType[] columnTypes)
        {
            IPage page = await this.GetPage(pageId, tran, PageType.MixedPage, columnTypes);

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

        public async Task Checkpoint()
        {
            foreach (IPage page in bufferPool.GetAllDirtyPages())
            {
                await FlushPage(page);
            }

            foreach (IPage page in this.AllocatationMapPages)
            {
                await FlushPage(page);
            }
        }

        public List<IntegerOnlyPage> GetAllocationMapFirstPage() => this.AllocatationMapPages;

        public void Dispose()
        {
            this.persistedStream.Dispose();
        }

        public ILockManager GetLockManager() => this.lockManager;
    }
}
