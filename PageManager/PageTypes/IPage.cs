namespace PageManager
{
    public interface IPage
    {
        public byte[] GetContent();
        public uint SizeInBytes();
        public ulong PageId();
        public PageType PageType();
    }
}
