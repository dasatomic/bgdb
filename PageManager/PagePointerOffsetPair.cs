using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace PageManager
{
    [StructLayout(LayoutKind.Sequential)]
    public struct PagePointerOffsetPair : IComparable<PagePointerOffsetPair>
    {
        public const uint Size = sizeof(long) + sizeof(int);

        public long PageId;
        public int OffsetInPage;

        public PagePointerOffsetPair(long pageId, int offsetInPage)
        {
            this.PageId = pageId;
            this.OffsetInPage = offsetInPage;
        }

        public int CompareTo([AllowNull] PagePointerOffsetPair other)
        {
            // For now not supported.
            throw new NotImplementedException();
        }
    }
}
