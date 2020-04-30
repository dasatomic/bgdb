namespace PageManager
{
    public interface IPage
    {
        public byte[] GetContent();
        public uint SizeInBytes();
        public ulong PageId();
        public PageType PageType();

        protected const uint PageIdPosition = 0;
        protected const uint PageSizePosition = 8;
        protected const uint PageTypePosition = 12;
        protected const uint NumOfRowsPosition = 16;
        protected const uint FirstElementPosition = 20;
    }
}
