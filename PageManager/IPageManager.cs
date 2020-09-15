using LockManager;
using PageManager.PageTypes;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PageManager
{
    public interface IBootPageAllocator
    {
        Task<IPage> AllocatePageBootPage(PageType pageType, ColumnInfo[] columnTypes, ITransaction tran);
        bool BootPageInitialized();

        public const ulong BootPageId = 0;
    }

    public interface IPageManager :  IAllocateStringPage, IAllocateMixedPage, IBootPageAllocator, IDisposable
    {
        // TODO: Get page shouldn't care about ColumnInfos.
        // This should be read directly from the disk.
        public Task<IPage> GetPage(ulong pageId, ITransaction tran, PageType pageType, ColumnInfo[] columnTypes);
        public Task<IPage> AllocatePage(PageType pageType, ColumnInfo[] columnTypes, ulong prevPageId, ulong nextPageId, ITransaction tran);
        public Task<IPage> AllocatePage(PageType pageType, ColumnInfo[] columnTypes, ulong prevPageId, ulong nextPageId, ulong pageId, ITransaction tran);
        public ulong PageCount();
        public Task Checkpoint();
        public List<BitTrackingPage> GetAllocationMapFirstPage();
        public ILockManager GetLockManager();
    }
}
