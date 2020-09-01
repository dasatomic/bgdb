using System.Runtime.InteropServices;

namespace PageManager
{
    [StructLayout(LayoutKind.Sequential)]
    public struct PagePointerOffsetPair
    {
        public const uint Size = sizeof(long) + sizeof(int);

        public long PageId;
        public int OffsetInPage;

        public PagePointerOffsetPair(long pageId, int offsetInPage)
        {
            this.PageId = pageId;
            this.OffsetInPage = offsetInPage;
        }
    }
}
