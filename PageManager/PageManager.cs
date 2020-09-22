using LockManager;
using LockManager.LockImplementation;
using LogManager;
using PageManager.PageTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PageManager
{
    public class PageManager : IPageManager
    {
        private uint pageSize;
        private long lastUsedPageId = 2;
        private readonly IPageEvictionPolicy pageEvictionPolicy;
        private readonly IPersistedStream persistedStream;
        private readonly IBufferPool bufferPool;

        private const int AllocationMapPageId = 1;
        private readonly List<BitTrackingPage> AllocatationMapPages;
        private readonly ILockManager lockManager;
        private InstrumentationInterface logger = null;
        private int pageCount;

        private const int DefaultBufferPoolSizeMb = 32;

        public PageManager(uint defaultPageSize, IPageEvictionPolicy evictionPolicy, IPersistedStream persistedStream)
            : this(defaultPageSize, evictionPolicy, persistedStream, new BufferPool(DefaultBufferPoolSizeMb, (int)defaultPageSize), new LockManager.LockManager(), new NoOpLogging())
        {
        }

        public PageManager(uint defaultPageSize, IPageEvictionPolicy evictionPolicy, IPersistedStream persistedStream, IBufferPool bufferPool, ILockManager lockManager, InstrumentationInterface logger)
        {
            this.pageSize = defaultPageSize;
            this.pageEvictionPolicy = evictionPolicy;
            this.persistedStream = persistedStream;
            this.lockManager = lockManager;
            this.logger = logger;

            this.bufferPool = bufferPool;

            this.AllocatationMapPages = new List<BitTrackingPage>();

            if (!this.persistedStream.IsInitialized())
            {
                logger.LogInfo("Initializing the persisted stream.");
                using (ITransaction tran = new NotLoggedTransaction())
                {
                    MixedPage allocationMapFirstPage = new MixedPage(pageSize, (ulong)AllocationMapPageId,  new [] { new ColumnInfo(ColumnType.Int) }, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, new byte[4096], 0, tran);
                    BitTrackingPage.NullifyMixedPage(allocationMapFirstPage, tran);

                    this.AllocatationMapPages.Add(new BitTrackingPage(allocationMapFirstPage));
                    this.persistedStream.MarkInitialized();
                }
            }
            else
            {
                // TODO: Read boot page.
                logger.LogInfo("Using already initialized stream.");
                ulong position = AllocationMapPageId * this.pageSize;
                MixedPage allocationMapFirstPage = (MixedPage)this.persistedStream.SeekAndRead(position, PageType.MixedPage, this.bufferPool, new ColumnInfo[] { new ColumnInfo(ColumnType.Int) }).Result;
                this.AllocatationMapPages.Add(new BitTrackingPage(allocationMapFirstPage));

                using (ITransaction tran = new NotLoggedTransaction())
                {
                    // TODO: Here we only iterate the first page.
                    // These pages need to be linked...
                    foreach (int _ in this.AllocatationMapPages.First().FindAllSet(tran))
                    {
                        this.pageCount++;
                    }
                }
            }
        }

        public async Task<IPage> AllocatePage(PageType pageType, ulong prevPageId, ulong nextPageId, ITransaction tran)
        {
            return await AllocatePage(pageType, null, prevPageId, nextPageId, tran).ConfigureAwait(false);
        }

        public async Task<IPage> AllocatePage(PageType pageType, ColumnInfo[] columnTypes, ulong prevPageId, ulong nextPageId, ITransaction tran)
        {
            long newPageId = Interlocked.Increment(ref lastUsedPageId);
            return await AllocatePage(pageType, columnTypes, prevPageId, nextPageId, (ulong)newPageId++, tran).ConfigureAwait(false);
        }

        public async Task<IPage> AllocatePageBootPage(PageType pageType, ColumnInfo[] columnTypes, ITransaction tran)
        {
            return await AllocatePage(pageType, columnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, IBootPageAllocator.BootPageId, tran).ConfigureAwait(false);
        }

        public async Task<IPage> AllocatePage(PageType pageType, ColumnInfo[] columnTypes, ulong prevPageId, ulong nextPageId, ulong pageId, ITransaction tran)
        {
            logger.LogDebug($"Allocating new page {pageId}");
            IPage page;

            using Releaser releaser = await tran.AcquireLockWithCallerOwnership(pageId, LockTypeEnum.Exclusive).ConfigureAwait(false);

            (Memory<byte> memory, ulong token) = this.bufferPool.GetMemory();

            page = pageType switch
            {
                PageType.StringPage => new StringOnlyPage(pageSize, pageId, prevPageId, nextPageId, tran),
                PageType.MixedPage => new MixedPage(pageSize, pageId, columnTypes, prevPageId, nextPageId, memory, token, tran),
                _ => throw new ArgumentException("Unknown page type")
            };

            Interlocked.Increment(ref this.pageCount);

            if (prevPageId != PageManagerConstants.NullPageId)
            {
                tran.VerifyLock(prevPageId, LockTypeEnum.Exclusive);
                IPage prevPage = await GetPage(prevPageId, tran, pageType, columnTypes).ConfigureAwait(false);

                if (prevPage.NextPageId() != page.NextPageId())
                {
                    throw new ArgumentException("Breaking the link");
                }

                // TODO: Set next page id needs to be logged as well...
                prevPage.SetNextPageId(page.PageId());
            }

            if (nextPageId != PageManagerConstants.NullPageId)
            {
                IPage nextPage = await GetPage(nextPageId, tran, pageType, columnTypes).ConfigureAwait(false);

                if (nextPage.PrevPageId() != page.PrevPageId())
                {
                    throw new ArgumentException("breaking the link");
                }

                nextPage.SetPrevPageId(page.PageId());
            }

            await RecordUsageAndEvict(page.PageId(), tran).ConfigureAwait(false);

            bufferPool.AddPage(page);

            AddToAllocationMap(page);

            return page;
        }

        private async Task RecordUsageAndEvict(ulong pageId, ITransaction tran)
        {
            ulong[] pageIdsToEvict = pageEvictionPolicy.RecordUsageAndEvict(pageId).ToArray();

            foreach (ulong pageIdToEvict in pageIdsToEvict)
            {
                if (tran.AmIHoldingALock(pageIdToEvict, out LockTypeEnum heldLockType))
                {
                    // If this page is needed for myself don't release the lock.
                    // Just ignore for now.
                    continue;
                }

                using (Releaser lckReleaser = await tran.AcquireLockWithCallerOwnership(pageIdToEvict, LockTypeEnum.Exclusive).ConfigureAwait(false))
                {
                    logger.LogDebug($"Evicting page {pageIdToEvict}");
                    IPage pageToEvict = this.bufferPool.GetPage(pageIdToEvict);

                    // Somebody came before us and evicted the page.
                    if (pageToEvict == null)
                    {
                        continue;
                    }

                    await this.FlushPage(pageToEvict).ConfigureAwait(false);

                    bufferPool.EvictPage(pageToEvict.PageId(), pageToEvict.GetBufferPoolToken());

                    logger.LogDebug($"Page {pageIdToEvict} evicted from buffer pool and flushed to the disk.");
                }
            }
        }

        private void AddToAllocationMap(IPage page)
        {
            // TODO: This needs to be revisited.
            // It is not ok to use Not logged transaction here.
            // it might be ok to use nested/inner transaction.
            // it is fine to leave the page even if user transaction is rolledbacked.
            BitTrackingPage AMPage = this.AllocatationMapPages.Last();
            int maxFit = AMPage.MaxItemCount();
            uint allocationMapPage = (uint)(page.PageId() / (uint)maxFit);

            if (allocationMapPage == this.AllocatationMapPages.Count)
            {
                using (ITransaction gamAllocTran = new NotLoggedTransaction())
                {
                    // TODO: Need to keep the list linked here.
                    (Memory<byte> memory, ulong token) = this.bufferPool.GetMemory();
                    MixedPage newAmPage = new MixedPage(
                        pageSize,
                        (ulong)AllocationMapPageId + (ulong)(maxFit * this.AllocatationMapPages.Count),
                        new ColumnInfo[] { new ColumnInfo(ColumnType.Int) },
                        PageManagerConstants.NullPageId,
                        PageManagerConstants.NullPageId,
                        memory,
                        token,
                        gamAllocTran);
                    BitTrackingPage.NullifyMixedPage(newAmPage, gamAllocTran);

                    this.AllocatationMapPages.Add(new BitTrackingPage(newAmPage));
                }
            }

            using (ITransaction gamUpdateTran = new NotLoggedTransaction())
            {
                BitTrackingPage gamPage = this.AllocatationMapPages.ElementAt((int)allocationMapPage);
                int positionInPage = (int)(page.PageId() % (uint)maxFit);

                gamPage.Set(positionInPage, gamUpdateTran);
            }
        }

        public async Task<IPage> GetPage(ulong pageId, ITransaction tran, PageType pageType, ColumnInfo[] columnTypes)
        {
            logger.LogDebug($"Fetching page {pageId}");
            tran.VerifyLock(pageId, LockTypeEnum.Shared);

            IPage page = this.bufferPool.GetPage(pageId);

            if (page == null)
            {
                // It is not sufficient to have shared lock here...

                logger.LogDebug($"Page {pageId} not present in buffer pool. Reading from disk.");
                page = await this.FetchPage(pageId, pageType, columnTypes).ConfigureAwait(false);
                this.bufferPool.AddPage(page);
            }
            else
            {
                logger.LogDebug($"Page {pageId} present in buffer pool.");
            }

            await RecordUsageAndEvict(pageId, tran).ConfigureAwait(false);

            return page;
        }

        internal async Task FlushPage(IPage page)
        {
            logger.LogDebug($"Flushing page {page.PageId()}. State IsDirty {page.IsDirty()}");
            if (page.IsDirty())
            {
                ulong position = page.PageId() * this.pageSize;
                await this.persistedStream.SeekAndWrite(position, page).ConfigureAwait(false);
                page.ResetDirty();
            }
        }

        internal async Task<IPage> FetchPage(ulong pageId, PageType pageType, ColumnInfo[] columnTypes)
        {
            ulong position = pageId * this.pageSize;
            return await this.persistedStream.SeekAndRead(position, pageType, this.bufferPool, columnTypes).ConfigureAwait(false);
        }

        public async Task<StringOnlyPage> AllocatePageStr(ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = await AllocatePage(PageType.StringPage, prevPage, nextPage, tran).ConfigureAwait(false);
            return (StringOnlyPage)page;
        }

        public async Task<StringOnlyPage> GetPageStr(ulong pageId, ITransaction tran)
        {
            IPage page = await this.GetPage(pageId, tran, PageType.StringPage, null).ConfigureAwait(false);

            if (page.PageType() != PageType.StringPage)
            {
                throw new InvalidCastException("Can't cast to string page");
            }

            return (StringOnlyPage)page;
        }

        public async Task<MixedPage> AllocateMixedPage(ColumnInfo[] columnTypes, ulong prevPage, ulong nextPage, ITransaction tran)
        {
            IPage page = await AllocatePage(PageType.MixedPage, columnTypes, prevPage, nextPage, tran).ConfigureAwait(false);
            return (MixedPage)page;
        }

        public async Task<MixedPage> GetMixedPage(ulong pageId, ITransaction tran, ColumnInfo[] columnTypes)
        {
            IPage page = await this.GetPage(pageId, tran, PageType.MixedPage, columnTypes).ConfigureAwait(false);

            if (page.PageType() != PageType.MixedPage)
            {
                throw new InvalidCastException("Can't cast to mixed page");
            }

            return (MixedPage)page;
        }

        public bool BootPageInitialized()
        {
            return this.bufferPool.GetPage(IBootPageAllocator.BootPageId) != null;
        }

        public ulong PageCount() => (ulong)this.pageCount;

        public async Task Checkpoint()
        {
            foreach (IPage page in bufferPool.GetAllDirtyPages())
            {
                await FlushPage(page).ConfigureAwait(false);
            }

            foreach (BitTrackingPage page in this.AllocatationMapPages)
            {
                await FlushPage(page.GetStoragePage()).ConfigureAwait(false);
            }
        }

        public List<BitTrackingPage> GetAllocationMapFirstPage() => this.AllocatationMapPages;

        public void Dispose()
        {
            this.persistedStream.Dispose();
        }

        public ILockManager GetLockManager() => this.lockManager;
    }
}
