namespace PageManager
{
    class MixedPage : IPage
    {
        private readonly uint pageSize;
        private readonly ulong pageId;

        // Byte representation:
        // [0-7] PageId
        // [8-11] PageSize
        // [12-15] PageType
        protected byte[] content;

        protected const uint PageIdPosition = 0;
        protected const uint PageSizePosition = 8;
        protected const uint PageTypePosition = 12;
        protected const uint NumOfRowsPosition = 16;
        protected const uint ElementTypesPosition = 20;

        public MixedPage(uint pageSize, ulong pageId, ColumnType[] columnTypes)
        {
            this.pageSize = pageSize;
            this.pageId = pageId;

            this.content = new byte[pageSize];
        }

        public void Serialize(object obj)
        {

        }

        public byte[] GetContent() => this.content;

        public ulong PageId() => this.pageId;

        public PageType PageType() => PageManager.PageType.MixedPage;

        public uint SizeInBytes() => this.pageSize;
    }
}
