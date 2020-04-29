namespace PageManager
{
    public struct PagePointerPair
    {
        public const uint Size = sizeof(long) + sizeof(int);

        public long PageId;
        public int OffsetInPage;

        public PagePointerPair(long pageId, int offsetInPage)
        {
            this.PageId = pageId;
            this.OffsetInPage = offsetInPage;
        }
    }
}
