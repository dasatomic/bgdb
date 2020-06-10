using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PageManager
{
    public interface IBootPageAllocator
    {
        Task<IPage> AllocatePageBootPage(PageType pageType, ColumnType[] columnTypes, ITransaction tran);
        bool BootPageInitialized();

        public const ulong BootPageId = 0;
    }

    public interface IPageManager :  IAllocateIntegerPage, IAllocateDoublePage, IAllocateStringPage, IAllocateLongPage, IAllocateMixedPage, IBootPageAllocator, IDisposable
    {
        public Task<IPage> GetPage(ulong pageId, ITransaction tran, PageType pageType, ColumnType[] columnTypes);
        public Task<IPage> AllocatePage(PageType pageType, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId, ITransaction tran);
        public Task<IPage> AllocatePage(PageType pageType, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId, ulong pageId, ITransaction tran);
        public ulong PageCount();
        public Task Checkpoint();
        public List<IntegerOnlyPage> GetAllocationMapFirstPage();
    }
}
