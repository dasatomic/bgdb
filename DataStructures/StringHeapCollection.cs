using PageManager;

namespace DataStructures
{
    public interface HeapWithOffsets<T>
    {
        PagePointerOffsetPair Add(T item, ITransaction tran);
        T Fetch(PagePointerOffsetPair loc, ITransaction tran);
    }

    public class StringHeapCollection : HeapWithOffsets<char[]>
    {
        IAllocateStringPage allocator;
        private ulong collectionRootPageId;

        public StringHeapCollection(IAllocateStringPage allocator, ITransaction tran)
        {
            this.allocator = allocator;
            this.collectionRootPageId = this.allocator.AllocatePageStr(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).PageId();
        }

        public StringHeapCollection(IAllocateStringPage allocator, IPage initialPage)
        {
            this.allocator = allocator;
            this.collectionRootPageId = initialPage.PageId();
        }

        public PagePointerOffsetPair Add(char[] item, ITransaction tran)
        {
            StringOnlyPage currPage = null;
            uint offset;
            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = allocator.GetPageStr(currPageId, tran);
                if (currPage.CanFit(item))
                {
                    offset = currPage.MergeWithOffsetFetch(item);
                    return new PagePointerOffsetPair((long)currPage.PageId(), (int)offset);
                }
            }

            currPage = this.allocator.AllocatePageStr(currPage.PageId(), PageManagerConstants.NullPageId, tran);
            offset = currPage.MergeWithOffsetFetch(item);
            return new PagePointerOffsetPair((long)currPage.PageId(), (int)offset);
        }

        public char[] Fetch(PagePointerOffsetPair loc, ITransaction tran)
        {
            StringOnlyPage page = allocator.GetPageStr((ulong)loc.PageId, tran);
            return page.FetchWithOffset((uint)loc.OffsetInPage);
        }
    }
}
