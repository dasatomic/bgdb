using System;
using PageManager;

namespace MetadataManager
{
    public interface UnorderedListCollection<T>
    {
        ulong Count();
        void Add(T item);
    }

    public class PageListCollection : UnorderedListCollection<RowsetHolder>
    {
        private ulong collectionRootPageId;
        private IAllocateMixedPage pageAllocator;
        private ColumnType[] columnTypes;

        public PageListCollection(IAllocateMixedPage pageAllocator, ColumnType[] columnTypes)
        {
            if (pageAllocator == null || columnTypes == null || columnTypes.Length == 0)
            {
                throw new ArgumentNullException();
            }

            this.collectionRootPageId = pageAllocator.AllocateMixedPage(columnTypes, 0, 0).PageId();
            this.pageAllocator = pageAllocator;
            this.columnTypes = columnTypes;
        }

        public ulong Count()
        {
            ulong rowCount = 0;

            IPage currPage;
            for (ulong currPageId = collectionRootPageId; currPageId != 0; currPageId = currPage.NextPageId())
            {
                currPage = pageAllocator.GetMixedPage(currPageId);
                rowCount += currPage.RowCount();
            }

            return rowCount;
        }

        public void Add(RowsetHolder item)
        {
            MixedPage currPage = null;
            for (ulong currPageId = collectionRootPageId; currPageId != 0; currPageId = currPage.NextPageId())
            {
                currPage = pageAllocator.GetMixedPage(currPageId);
                if (currPage.CanFit(item))
                {
                    currPage.Merge(item);
                    return;
                }
            }

            currPage = this.pageAllocator.AllocateMixedPage(this.columnTypes, currPage.PageId(), 0);
            currPage.Merge(item);
        }
    }
}
