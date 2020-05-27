using System.Runtime.CompilerServices;
[assembly:InternalsVisibleTo("PageManagerTests")]

namespace PageManager
{
    public interface IPage
    {
        public uint SizeInBytes();
        public ulong PageId();
        public PageType PageType();
        public ulong PrevPageId();
        public ulong NextPageId();
        public void SetNextPageId(ulong nextPageId);
        public void SetPrevPageId(ulong prevPageId);
        public uint RowCount();
        public uint MaxRowCount();
        public void RedoLog(ILogRecord record, ITransaction tran);
        public void UndoLog(ILogRecord record, ITransaction tran);

        internal const uint PageIdPosition = 0;
        internal const uint PageSizePosition = 8;
        internal const uint PageTypePosition = 12;
        internal const uint NumOfRowsPosition = 16;
        internal const uint PrevPagePosition = 20;
        internal const uint NextPagePosition = 28;
        internal const uint FirstElementPosition = 36;
    }
}
