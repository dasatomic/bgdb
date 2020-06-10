using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using PageManager;

namespace DataStructures
{
    public interface UnorderedListCollection<T>
    {
        Task<ulong> Count(ITransaction tran);
        Task Add(T item, ITransaction tran);
        Task<List<T>> Where(Func<T, bool> filter, ITransaction tran);
        Task<U> Max<U>(Func<T, U> projector, U startMin, ITransaction tran) where U : IComparable;
        Task<bool> IsEmpty(ITransaction tran);
        IAsyncEnumerable<T> Iterate(ITransaction tran);
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

            this.collectionRootPageId = pageAllocator.AllocateMixedPage(columnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).Result.PageId();
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

        public async Task<ulong> Count(ITransaction tran)
        {
            ulong rowCount = 0;

            IPage currPage;
            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = await pageAllocator.GetMixedPage(currPageId, tran, this.columnTypes);
                rowCount += currPage.RowCount();
            }

            return rowCount;
        }

        public async Task Add(RowsetHolder item, ITransaction tran)
        {
            MixedPage currPage = null;
            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = await pageAllocator.GetMixedPage(currPageId, tran, this.columnTypes);
                if (currPage.CanFit(item))
                {
                    currPage.Merge(item, tran);
                    return;
                }
            }

            currPage = await this.pageAllocator.AllocateMixedPage(this.columnTypes, currPage.PageId(), PageManagerConstants.NullPageId, tran);
            currPage.Merge(item, tran);
        }

        public async Task<List<RowsetHolder>> Where(Func<RowsetHolder, bool> filter, ITransaction tran)
        {
            MixedPage currPage;
            List<RowsetHolder> result = new List<RowsetHolder>();
            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = await pageAllocator.GetMixedPage(currPageId, tran, this.columnTypes);
                RowsetHolder holder = currPage.Fetch();

                if (filter(holder))
                {
                    result.Add(holder);
                }
            }

            return result;
        }

        public async Task<U> Max<U>(Func<RowsetHolder, U> projector, U startMin, ITransaction tran) where U : IComparable
        {
            MixedPage currPage;
            U max = startMin;

            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = await pageAllocator.GetMixedPage(currPageId, tran, this.columnTypes);
                RowsetHolder holder = currPage.Fetch();

                U curr = projector(holder);

                if (curr.CompareTo(max) == 1)
                {
                    max = curr;
                }
            }

            return max;
        }

        public async IAsyncEnumerable<RowsetHolder> Iterate(ITransaction tran)
        {
            MixedPage currPage;
            for (ulong currPageId = collectionRootPageId; currPageId != PageManagerConstants.NullPageId; currPageId = currPage.NextPageId())
            {
                currPage = await pageAllocator.GetMixedPage(currPageId, tran, this.columnTypes);
                RowsetHolder holder = currPage.Fetch();

                yield return holder;
            }
        }

        public async Task<bool> IsEmpty(ITransaction tran)
        {
            return await this.Count(tran) == 0;
        }
    }
}
