namespace PageManager
{
    public interface IBootPageAllocator
    {
        IPage AllocatePageBootPage(PageType pageType, ColumnType[] columnTypes, ITransaction tran);
        bool BootPageInitialized();

        public const ulong BootPageId = ulong.MaxValue;
    }

    public interface IPageManager :  IAllocateIntegerPage, IAllocateDoublePage, IAllocateStringPage, IAllocateLongPage, IAllocateMixedPage, IBootPageAllocator
    {
    }
}
