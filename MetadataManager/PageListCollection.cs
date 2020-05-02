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
        private MixedPage collectionRoot = null;
        private IAllocateMixedPage pageAllocator;
        private ColumnType[] columnTypes;

        public PageListCollection(IAllocateMixedPage pageAllocator, ColumnType[] columnTypes)
        {
            if (pageAllocator == null || columnTypes == null || columnTypes.Length == 0)
            {
                throw new ArgumentNullException();
            }

            collectionRoot = pageAllocator.AllocateMixedPage(columnTypes, 0, 0);
            this.pageAllocator = pageAllocator;
            this.columnTypes = columnTypes;
        }

        public ulong Count()
        {
            IPage currPage = collectionRoot;
            ulong rowCount = 0;
            ulong currPageId = currPage.PageId();

            for (; currPageId != 0; currPageId = currPage.NextPageId())
            {
                currPage = pageAllocator.GetMixedPage(currPageId);
                rowCount += currPage.RowCount();
            }

            return rowCount;
        }

        public void Add(RowsetHolder item)
        {
            MixedPage currPage = collectionRoot;
            ulong currPageId = currPage.PageId();

            for (; currPageId != 0; currPageId = currPage.NextPageId())
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
