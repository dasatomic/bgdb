namespace PageManager
{
    public interface IPage
    {
        public byte[] GetContent();
        public uint SizeInBytes();
        public ulong PageId();
        public PageType PageType();
        public ulong PrevPageId();
        public ulong NextPageId();
        public void SetNextPageId(ulong nextPageId);
        public void SetPrevPageId(ulong prevPageId);
        public uint RowCount();
        public uint MaxRowCount();

        protected const uint PageIdPosition = 0;
        protected const uint PageSizePosition = 8;
        protected const uint PageTypePosition = 12;
        protected const uint NumOfRowsPosition = 16;
        protected const uint PrevPagePosition = 20;
        protected const uint NextPagePosition = 28;
        protected const uint FirstElementPosition = 32;
    }
}
