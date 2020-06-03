using System;
using System.Collections;
using System.Collections.Generic;
using PageManager;

namespace MetadataManager
{
    public interface UnorderedListCollection<T>
    {
        ulong Count(ITransaction tran);
        void Add(T item, ITransaction tran);
        List<T> Where(Func<T, bool> filter, ITransaction tran);
        U Max<U>(Func<T, U> projector, U startMin, ITransaction tran) where U : IComparable;
        bool IsEmpty(ITransaction tran);
        IEnumerable<T> Iterate(ITransaction tran);
    }

    public class PageListCollection : UnorderedListCollection<RowsetHolder>
    {
        private ulong collectionRootPageId;
        private IAllocateMixedPage pageAllocator;
        private ColumnType[] columnTypes;

        public PageListCollection(IAllocateMixedPage pageAllocator, ColumnType[] columnTypes, ITransaction tran)
        {
            if (pageAllocator == null || columnTypes == null || columnTypes.Length == 0)
            {
                throw new ArgumentNullException();
            }

            this.collectionRootPageId = pageAllocator.AllocateMixedPage(columnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).PageId();
            this.pageAllocator = pageAllocator;
            this.columnTypes = columnTypes;
        }

        public PageListCollection(IAllocateMixedPage pageAllocator, ColumnType[] columnTypes, IPage initialPage)
        {
            if (pageAllocator == null || columnTypes == null || columnTypes.Length == 0)
            {
                throw new ArgumentNullException();
            }

            this.collectionRootPageId = initialPage.PageId();
            this.pageAllocator = pageAllocator;
            this.columnTypes = columnTypes;
        }

        public ulong Count(ITransaction tran)
        {
            ulong rowCount = 0;

            IPage currPage;
            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = pageAllocator.GetMixedPage(currPageId, tran);
                rowCount += currPage.RowCount();
            }

            return rowCount;
        }

        public void Add(RowsetHolder item, ITransaction tran)
        {
            MixedPage currPage = null;
            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = pageAllocator.GetMixedPage(currPageId, tran);
                if (currPage.CanFit(item))
                {
                    currPage.Merge(item, tran);
                    return;
                }
            }

            currPage = this.pageAllocator.AllocateMixedPage(this.columnTypes, currPage.PageId(), PageManagerConstants.NullPageId, tran);
            currPage.Merge(item, tran);
        }

        public List<RowsetHolder> Where(Func<RowsetHolder, bool> filter, ITransaction tran)
        {
            MixedPage currPage;
            List<RowsetHolder> result = new List<RowsetHolder>();
            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = pageAllocator.GetMixedPage(currPageId, tran);
                RowsetHolder holder = currPage.Fetch();

                if (filter(holder))
                {
                    result.Add(holder);
                }
            }

            return result;
        }

        public U Max<U>(Func<RowsetHolder, U> projector, U startMin, ITransaction tran) where U : IComparable
        {
            MixedPage currPage;
            U max = startMin;

            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = pageAllocator.GetMixedPage(currPageId, tran);
                RowsetHolder holder = currPage.Fetch();

                U curr = projector(holder);

                if (curr.CompareTo(max) == 1)
                {
                    max = curr;
                }
            }

            return max;
        }

        public IEnumerable<RowsetHolder> Iterate(ITransaction tran)
        {
            MixedPage currPage;
            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = pageAllocator.GetMixedPage(currPageId, tran);
                RowsetHolder holder = currPage.Fetch();

                yield return holder;
            }
        }

        public bool IsEmpty(ITransaction tran)
        {
            return this.Count(tran) == 0;
        }
    }
}
