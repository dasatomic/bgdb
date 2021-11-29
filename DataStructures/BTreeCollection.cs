using DataStructures.Exceptions;
using LockManager.LockImplementation;
using PageManager;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using static PageManager.ProjectExtendInfo;

namespace DataStructures
{
    public class BTreeCollection : IPageCollection<RowHolder>
    {
        private ulong collectionRootPageId;
        private IAllocateMixedPage pageAllocator;
        // Not including pointers between pages.
        private ColumnInfo[] columnTypes;
        // Including pointers between pages.
        private ColumnInfo[] btreeColumnTypes;
        private int maxElemsPerPage;
        private int pagePointerRowPosition;

        private Func<RowHolder, RowHolder, int> indexComparer;

        private ProjectExtendInfo projectionRemoveIndexCol;

        private Func<MixedPage, string> debugPrintPage;

        private int indexPosition;

        public BTreeCollection(IAllocateMixedPage pageAllocator, ColumnInfo[] columnTypes, ITransaction tran, Func<RowHolder, RowHolder, int> indexComparer, int indexPosition)
        {
            if (pageAllocator == null || columnTypes == null || columnTypes.Length == 0 || tran == null)
            {
                throw new ArgumentNullException();
            }

            this.pageAllocator = pageAllocator;
            this.columnTypes = columnTypes;
            this.indexPosition = indexPosition;

            // Add PagePointer to the end of each row.
            // Page pointer will be used as pointer in btree.
            // Prev page pointer will point to the left side of the first key.
            // Next page pointer is used to encode additional info.
            // For now next page is used to encode whether the page is leaf.
            this.btreeColumnTypes = new ColumnInfo[columnTypes.Length + 1];
            Array.Copy(columnTypes, this.btreeColumnTypes, columnTypes.Length);
            this.btreeColumnTypes[columnTypes.Length] = new ColumnInfo(ColumnType.PagePointer);
            MixedPage rootPage = pageAllocator.AllocateMixedPage(this.btreeColumnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran).Result;
            this.collectionRootPageId = rootPage.PageId();

            uint maxRowCount = rootPage.MaxRowCount();

            // In btree we can fit max 2*t + 1 elements.
            // If we can fit even number we will keep the space unused.
            this.maxElemsPerPage = (int)maxRowCount;
            if (maxRowCount % 2 == 0)
            {
                this.maxElemsPerPage--;
            }

            this.indexComparer = indexComparer;

            this.pagePointerRowPosition = this.columnTypes.Length;

            MappingType[] mps = new MappingType[columnTypes.Length];
            int[] prjs = new int[columnTypes.Length];
            for (int i = 0; i < prjs.Length; i++)
            {
                mps[i] = MappingType.Projection;
                prjs[i] = i;
            }

            this.projectionRemoveIndexCol = new ProjectExtendInfo(mps, prjs, new ColumnInfo[0]);
        }

        public async Task Add(RowHolder item, ITransaction tran)
        {
            // Extend RowHolder with page pointer.
            // TODO: This is not the fastest option.
            MappingType[] mappingTypes = new MappingType[item.ColumnPosition.Length + 1];
            mappingTypes[item.ColumnPosition.Length] = MappingType.Extension;
            int[] projectionOrder = new int[item.ColumnPosition.Length];
            for (int i = 0; i < projectionOrder.Length; i++)
            {
                projectionOrder[i] = i;
            }
            ColumnInfo[] extension = new ColumnInfo[1] { new ColumnInfo(ColumnType.PagePointer) };

            ProjectExtendInfo extensionInfo = new ProjectExtendInfo(mappingTypes, projectionOrder, extension);

            RowHolder itemToInsert = item.ProjectAndExtend(extensionInfo);
            itemToInsert.SetField<ulong>(this.pagePointerRowPosition, PageManagerConstants.NullPageId);

            // Tree traversal.
            ulong currPageId = collectionRootPageId;
            ulong prevPageId = PageManagerConstants.NullPageId;

            bool insertFinished = false;

            while (!insertFinished)
            {
                Debug.Assert(currPageId != prevPageId);
                using Releaser lck = await tran.AcquireLock(currPageId, LockManager.LockTypeEnum.Shared).ConfigureAwait(false);
                MixedPage currPage = await pageAllocator.GetMixedPage(currPageId, tran, this.btreeColumnTypes).ConfigureAwait(false);

                MixedPage prevPage = null;
                Releaser prevPageReleaser = Releaser.FakeReleaser;
                if (prevPageId != PageManagerConstants.NullPageId)
                {
                    prevPageReleaser = await tran.AcquireLock(prevPageId, LockManager.LockTypeEnum.Shared).ConfigureAwait(false);
                    prevPage = await pageAllocator.GetMixedPage(prevPageId, tran, this.btreeColumnTypes).ConfigureAwait(false);
                }

                if (debugPrintPage != null)
                {
                    string debugInfo = debugPrintPage(currPage);
                }

                if (this.IsLeaf(currPage))
                {
                    // leaf.
                    lck.Dispose();

                    using Releaser writeLock = await tran.AcquireLock(currPage.PageId(), LockManager.LockTypeEnum.Exclusive).ConfigureAwait(false);

                    if (currPage.RowCount() < this.maxElemsPerPage)
                    {
                        int pos = currPage.InsertOrdered(itemToInsert, tran, this.btreeColumnTypes, this.indexComparer);
                        Debug.Assert(pos >= 0);

                        // all done.
                        insertFinished = true;
                    }
                    else
                    {
                        // need to split.
                        MixedPage newPageForSplit = await pageAllocator.AllocateMixedPage(this.btreeColumnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);

                        RowHolder rowHolderForSplit = await this.SplitBtreePage(currPage, newPageForSplit, prevPage, tran);

                        // See if value should end up in left or right child.
                        int compareResult = this.indexComparer(itemToInsert, rowHolderForSplit);
                        if (compareResult < 0)
                        {
                            // left.
                            int pos = currPage.InsertOrdered(itemToInsert, tran, this.btreeColumnTypes, this.indexComparer);
                        }
                        else
                        {
                            // right.
                            int pos = newPageForSplit.InsertOrdered(itemToInsert, tran, this.btreeColumnTypes, this.indexComparer);
                        }

                        insertFinished = true;
                    }
                }
                else
                {
                    // non leaf
                    // If row count is = maxElemsPerPage - 2 then split and continue traversal.
                    if (currPage.RowCount() < this.maxElemsPerPage - 2)
                    {
                        // Just iterate.
                        ulong prevPagePointer = currPage.PrevPageId();
                        prevPageId = currPageId;
                        foreach (RowHolder rh in currPage.Fetch(tran))
                        {
                            int compareResult = this.indexComparer(itemToInsert, rh);

                            if (compareResult == 0)
                            {
                                throw new KeyAlreadyExists();
                            }
                            else if (compareResult < 0)
                            {
                                // follow the link.
                                currPageId = prevPagePointer;
                                break;
                            }
                            else
                            {
                                prevPagePointer = rh.GetField<ulong>(this.pagePointerRowPosition);
                                currPageId = prevPagePointer;
                            }
                        }
                    }
                    else
                    {
                        // Split the page.
                        MixedPage newPageForSplit = await pageAllocator.AllocateMixedPage(this.btreeColumnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);

                        RowHolder rowHolderForSplit = await this.SplitBtreePage(currPage, newPageForSplit, prevPage, tran);
                        int compareResult = this.indexComparer(itemToInsert, rowHolderForSplit);

                        // prev page either remains the same
                        // or it can become new root.

                        if (compareResult == 0)
                        {
                            throw new KeyAlreadyExists();
                        }
                        else if (compareResult < 0)
                        {
                            // Curr page remains the same.
                            // let the loop unfold.
                        }
                        else
                        {
                            // prev page remains the same.
                            currPageId = newPageForSplit.PageId();
                        }
                    }
                }

                prevPageReleaser.Dispose();
            }
        }

        public ColumnInfo[] GetColumnTypes()
        {
            return this.columnTypes;
        }

        public Task<bool> IsEmpty(ITransaction tran)
        {
            throw new NotImplementedException();
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            Stack<Tuple<ulong, int, Releaser>> stack = new Stack<Tuple<ulong, int, Releaser>>();
            stack.Push(Tuple.Create(this.collectionRootPageId, -1, Releaser.FakeReleaser));

            while (stack.Count != 0)
            {
                (ulong pageId, int posInPage, Releaser releaser) = stack.Pop();

                if (posInPage == -1)
                {
                    // first time visiting page.
                    releaser = await tran.AcquireLock(pageId, LockManager.LockTypeEnum.Shared).ConfigureAwait(false);
                    MixedPage currPage = await pageAllocator.GetMixedPage(pageId, tran, this.btreeColumnTypes).ConfigureAwait(false);

                    if (this.debugPrintPage != null)
                    {
                        string info = this.debugPrintPage(currPage);
                    }

                    // Push current element on the stack.
                    stack.Push(Tuple.Create(pageId, 0, releaser));
                    if (currPage.PrevPageId() != PageManagerConstants.NullPageId)
                    {
                        stack.Push(Tuple.Create(currPage.PrevPageId(), -1, Releaser.FakeReleaser));
                    }
                }
                else
                {
                    // We must have the lock at this moment.
                    tran.VerifyLock(pageId, LockManager.LockTypeEnum.Shared);
                    MixedPage currPage = await pageAllocator.GetMixedPage(pageId, tran, this.btreeColumnTypes).ConfigureAwait(false);

                    if (this.debugPrintPage != null)
                    {
                        string info = this.debugPrintPage(currPage);
                    }

                    // TODO: This is super slow.
                    // Just testing validity.
                    RowHolder rowHolder = currPage.Fetch(tran).ElementAt(posInPage);
                    RowHolder rhWithoutPointer = rowHolder.ProjectAndExtend(this.projectionRemoveIndexCol);
                    yield return rhWithoutPointer;

                    ulong childPointer = rowHolder.GetField<ulong>(this.pagePointerRowPosition);

                    if (posInPage < currPage.RowCount() - 1)
                    {
                        stack.Push(Tuple.Create(pageId, posInPage + 1, releaser));
                    }

                    if (childPointer != PageManagerConstants.NullPageId)
                    {
                        stack.Push(Tuple.Create(childPointer, -1, Releaser.FakeReleaser));
                    }

                    if (posInPage == currPage.RowCount() - 1)
                    {
                        // Done with this page.
                        releaser.Dispose();
                    }
                }
            }
        }

        public Task<U> Max<U>(Func<RowHolder, U> projector, U startMin, ITransaction tran) where U : IComparable
        {
            throw new NotImplementedException();
        }

        public IAsyncEnumerable<RowHolder> Where(Func<RowHolder, bool> filter, ITransaction tran)
        {
            throw new NotImplementedException();
        }

        private bool IsLeaf(IPage page)
        {
            // Lock must be already taken.
            ulong nextPageId = page.NextPageId();
            return Convert.ToBoolean(nextPageId & 1);
        }

        private void SetLeaf(IPage page, bool isLeaf)
        {
            // Lock must be already taken.
            ulong nextPageId = page.NextPageId();

            if (isLeaf)
            {
                nextPageId = nextPageId | 0x1;
            }
            else
            {
                nextPageId = nextPageId & ~1UL;
            }

            // TODO: Verify if this change is logged.
            page.SetNextPageId(nextPageId);
        }

        private async Task<RowHolder> SplitBtreePage(MixedPage currPage, MixedPage newPageForSplit, MixedPage prevPage, ITransaction tran)
        {
            if (prevPage != null)
            {
                Debug.Assert(currPage.PageId() != prevPage.PageId());
            }

            using Releaser writeLockNewPage = await tran.AcquireLock(newPageForSplit.PageId(), LockManager.LockTypeEnum.Exclusive).ConfigureAwait(false);

            RowHolder rowHolderForSplit = new RowHolder(this.btreeColumnTypes);
            currPage.SplitPage(newPageForSplit, ref rowHolderForSplit, this.maxElemsPerPage / 2, tran);

            // new node is inhering child of the split value.
            ulong pagePointerOfSplitValue = rowHolderForSplit.GetField<ulong>(this.pagePointerRowPosition);
            newPageForSplit.SetPrevPageId(pagePointerOfSplitValue);

            // Split value pointer now points to new node.
            rowHolderForSplit.SetField<ulong>(this.pagePointerRowPosition, newPageForSplit.PageId());

            this.SetLeaf(newPageForSplit, this.IsLeaf(currPage));

            if (prevPage != null)
            {
                int pos = prevPage.InsertOrdered(rowHolderForSplit, tran, this.btreeColumnTypes, this.indexComparer);

                // We know that there will be enough place since we proactivly clean parent nodes.
                Debug.Assert(pos >= 0);
                if (prevPage.PageId() != this.collectionRootPageId)
                {
                    Debug.Assert(pos >= this.maxElemsPerPage / 2 - 1);
                }
            }
            else
            {
                // If prev page is null we are in the root.
                // need to allocate.
                Debug.Assert(currPage.PageId() == this.collectionRootPageId);

                MixedPage newRoot = await pageAllocator.AllocateMixedPage(this.btreeColumnTypes, PageManagerConstants.NullPageId, PageManagerConstants.NullPageId, tran);
                this.SetLeaf(newRoot, false);
                using Releaser newRootLock = await tran.AcquireLock(newPageForSplit.PageId(), LockManager.LockTypeEnum.Exclusive).ConfigureAwait(false);
                int pos = newRoot.InsertOrdered(rowHolderForSplit, tran, this.btreeColumnTypes, this.indexComparer);
                Debug.Assert(pos == 0);

                newRoot.SetPrevPageId(currPage.PageId());

                // TODO: This again needs to be logged.
                // On metadata level we need to be able to recover new root after restart.
                this.collectionRootPageId = newRoot.PageId();
            }

            // Return value that split the nodes.
            return rowHolderForSplit;
        }

        public Task<ulong> Count(ITransaction tran)
        {
            throw new NotImplementedException();
        }

        public void SetDebugNodePrint(Func<MixedPage, string> debugPagePrint)
        {
            this.debugPrintPage = debugPagePrint;
        }

        public bool SupportsSeek() => true;

        public async IAsyncEnumerable<RowHolder> Seek<K>(K seekVal, ITransaction tran)
            where K : unmanaged, IComparable<K>
        {
            ulong currPageId = this.collectionRootPageId;
            while (true)
            {
                using Releaser lck = await tran.AcquireLock(currPageId, LockManager.LockTypeEnum.Shared).ConfigureAwait(false);
                MixedPage currPage = await pageAllocator.GetMixedPage(currPageId, tran, this.btreeColumnTypes).ConfigureAwait(false);

                ulong prevPointer = currPage.PrevPageId();
                foreach (RowHolder rh in currPage.Fetch(tran))
                {
                    K val = rh.GetField<K>(this.indexPosition);
                    ulong pagePointer = rh.GetField<ulong>(this.pagePointerRowPosition);

                    int compResult = seekVal.CompareTo(val);
                    if (compResult == -1)
                    {
                        // seek val is bigger.
                        // go down
                        // Seek val is smaller. Need to go left.
                        if (prevPointer == PageManagerConstants.NullPageId)
                        {
                            throw new KeyNotFound();
                        }
                        else
                        {
                            currPageId = prevPointer;
                        }

                        break;
                    }
                    else if (compResult == 0)
                    {
                        // found the val.
                        // For now we don't support key duplication.
                        RowHolder rhWithoutPointer = rh.ProjectAndExtend(this.projectionRemoveIndexCol);
                        yield return rhWithoutPointer;
                        yield break;
                    }
                    else
                    {
                        prevPointer = pagePointer;
                    }
                }

                // Reached the end. The val is bigger than anything else
                currPageId = prevPointer;
            }
        }
    }
}
