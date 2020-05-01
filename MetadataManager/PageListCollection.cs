using System;
using System.Collections.Generic;
using PageManager;
using System.Collections;

namespace MetadataManager
{
    public class PageListCollection : IList<RowsetHolder>
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

        private long CountItems()
        {
            IPage currPage = collectionRoot;
            long rowCount = 0;

            do
            {
                rowCount += currPage.RowCount();
            }
            while (currPage.NextPageId() != 0);

            return rowCount;
        }

        public int Count
        {
            get
            {
                return (int)CountItems();
            }
        }

        public bool IsReadOnly => false;

        public RowsetHolder this[int index] { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public int IndexOf(RowsetHolder item)
        {
            throw new NotImplementedException();
        }

        public void Insert(int index, RowsetHolder item)
        {
            uint neededSize = item.StorageSizeInBytes();
            throw new NotImplementedException();
        }

        public void Add(RowsetHolder item)
        {
            MixedPage currPage = collectionRoot;

            do
            {
                if (currPage.CanFit(item))
                {
                    currPage.Merge(item);
                    return;
                }
            }
            while (currPage.NextPageId() != 0);

            currPage = this.pageAllocator.AllocateMixedPage(this.columnTypes, currPage.PageId(), 0);
            currPage.Merge(item);
        }

        public bool Contains(RowsetHolder item)
        {
            throw new NotImplementedException();
        }

        public bool Remove(RowsetHolder item)
        {
            throw new NotImplementedException();
        }

        IEnumerator<RowsetHolder> IEnumerable<RowsetHolder>.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            throw new NotImplementedException();
        }

        public IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public void RemoveAt(int index)
        {
            throw new NotImplementedException();
        }

        public void CopyTo(RowsetHolder[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }
    }
}
