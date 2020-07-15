using LockManager.LockImplementation;
using PageManager;
using System.Threading.Tasks;

namespace DataStructures
{
    public interface HeapWithOffsets<T>
    {
        Task<PagePointerOffsetPair> Add(T item, ITransaction tran);
        Task<T> Fetch(PagePointerOffsetPair loc, ITransaction tran);
    }

    public class StringHeapCollection : HeapWithOffsets<char[]>
    {
        IAllocateStringPage allocator;
        private ulong collectionRootPageId;
        private ulong lastPageId;

        public StringHeapCollection(IAllocateStringPage allocator, ITransaction tran)
        {
            this.allocator = allocator;
            this.collectionRootPageId = this.allocator.AllocatePageStr(PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).Result.PageId();
            this.lastPageId = this.collectionRootPageId;
        }

        public StringHeapCollection(IAllocateStringPage allocator, IPage initialPage)
        {
            this.allocator = allocator;
            this.collectionRootPageId = initialPage.PageId();
            this.lastPageId = this.collectionRootPageId;
        }

        public async Task<PagePointerOffsetPair> Add(char[] item, ITransaction tran)
        {
            StringOnlyPage currPage = null;
            uint offset;
            for (ulong currPageId = this.lastPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                using Releaser lckReleaser = await tran.AcquireLock(currPageId, LockManager.LockTypeEnum.Exclusive);
                currPage = await allocator.GetPageStr(currPageId, tran);
                if (currPage.CanFit(item))
                {
                    offset = currPage.MergeWithOffsetFetch(item, tran);
                    return new PagePointerOffsetPair((long)currPage.PageId(), (int)offset);
                }
            }

            {
                using Releaser prevPage = await tran.AcquireLock(currPage.PageId(), LockManager.LockTypeEnum.Exclusive);

                if (currPage.NextPageId() != PageManagerConstants.NullPageId)
                {
                    prevPage.Dispose();
                    return await Add(item, tran);
                }
                else
                {
                    currPage = await this.allocator.AllocatePageStr(currPage.PageId(), PageManagerConstants.NullPageId, tran);
                    using Releaser lckReleaser = await tran.AcquireLock(currPage.PageId(), LockManager.LockTypeEnum.Exclusive);
                    offset = currPage.MergeWithOffsetFetch(item, tran);
                    this.lastPageId = currPage.PageId();
                    return new PagePointerOffsetPair((long)currPage.PageId(), (int)offset);
                }
            }
        }

        public async Task<char[]> Fetch(PagePointerOffsetPair loc, ITransaction tran)
        {
            using Releaser lckReleaser = await tran.AcquireLock((ulong)loc.PageId, LockManager.LockTypeEnum.Shared);
            StringOnlyPage page = await allocator.GetPageStr((ulong)loc.PageId, tran);
            return page.FetchWithOffset((uint)loc.OffsetInPage, tran);
        }
    }
}
