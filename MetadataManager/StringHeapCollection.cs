using PageManager;

namespace MetadataManager
{
    public interface HeapWithOffsets<T>
    {
        PagePointerOffsetPair Add(T item);
    }

    public class StringHeapCollection : HeapWithOffsets<char[]>
    {
        IAllocateStringPage allocator;
        private ulong collectionRootPageId;

        public StringHeapCollection(IAllocateStringPage allocator)
        {
            this.allocator = allocator;
            this.collectionRootPageId = this.allocator.AllocatePageStr(0, 0).PageId();
        }

        public StringHeapCollection(IAllocateStringPage allocator, IPage initialPage)
        {
            this.allocator = allocator;
            this.collectionRootPageId = initialPage.PageId();
        }

        public PagePointerOffsetPair Add(char[] item)
        {
            StringOnlyPage currPage = null;
            uint offset;
            for (ulong currPageId = collectionRootPageId; currPageId != 0; currPageId = currPage.NextPageId())
            {
                currPage = allocator.GetPageStr(currPageId);
                if (currPage.CanFit(item))
                {
                    offset = currPage.MergeWithOffsetFetch(item);
                    return new PagePointerOffsetPair((long)currPage.PageId(), (int)offset);
                }
            }

            currPage = this.allocator.AllocatePageStr(currPage.PageId(), 0);
            offset = currPage.MergeWithOffsetFetch(item);
            return new PagePointerOffsetPair((long)currPage.PageId(), (int)offset);
        }
    }
}
