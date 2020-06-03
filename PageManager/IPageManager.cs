namespace PageManager
{
    public interface IBootPageAllocator
    {
        IPage AllocatePageBootPage(PageType pageType, ColumnType[] columnTypes, ITransaction tran);
        bool BootPageInitialized();

        public const ulong BootPageId = 0;
    }

    public interface IPageManager :  IAllocateIntegerPage, IAllocateDoublePage, IAllocateStringPage, IAllocateLongPage, IAllocateMixedPage, IBootPageAllocator
    {
        public IPage GetPage(ulong pageId, ITransaction tran);
        public IPage AllocatePage(PageType pageType, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId, ITransaction tran);
        public IPage AllocatePage(PageType pageType, ColumnType[] columnTypes, ulong prevPageId, ulong nextPageId, ulong pageId, ITransaction tran);
        public ulong PageCount();
    }
}
