This is the starting point for inserting random keys into btree.

| Method | RowsInTableNumber | Mean |
|--------|----------|-------------|
| InsertRandomData | 10000 | 217.3 ms|
| InsertRandomData	| 50000	            | 1,236.9 ms	|
| InsertRandomData	| 100000	        | 2,759.3 ms	|
| InsertRandomData	| 200000	        | 6,043.7 ms	|


This is rather bad, but also the initial implementation only aimed at correctness.
Let's try to optimize the perf. The goal should be 1M inserts < 1s (~30X).

Just to add, measurements at this point are pretty lose - e.g. I will be writing this while the tests are running and eating precious CPU. Tests are run on amd 1600x, 32gb of ram and m2 ssd.

So let's start with the flame graph.

![](flameGraphStep1.png)

We can see that the most of the time is getting spent in RowsetHolder iterate method.

This is the current implementation:

        public IEnumerable<RowHolder> Iterate(ColumnInfo[] columnTypes)
        {
            for (int i = 0; i < this.maxRowCount; i++)
            {
                if (BitArray.IsSet(i, this.storage.Span))
                {
                    RowHolder rowHolder = new RowHolder(columnTypes);
                    GetRow(i, ref rowHolder);
                    yield return rowHolder;
                }
            }
        }

This indeed is super slow. We are allocating individual RowHolder for each row to be returned which puts a lot of pressure on memory allocation.

This code is getting called from:

        public override IEnumerable<RowHolder> Fetch(ITransaction tran)
        {
            tran.VerifyLock(this.pageId, LockManager.LockTypeEnum.Shared);

            lock (this.lockObject)
            {
                return  this.items.Iterate(this.columnTypes);
            }
        }


Idea here is to minimize needless copying and keep everything on RowSetHolder level.
Since btree implementation currently can't host duplicates we check for duplicates on every insert. This turns to be pretty costly. Instead of doing full fetch and then iterate and call functor on each element we will go with something like this:

        public bool ElemExists<T>(ColumnInfo[] columnTypes, T elem, int columnPos) where T : unmanaged, IComparable
        {
            ushort colPosition = 0;
            for (int i = 0; i < columnPos; i++)
            {
                colPosition += columnTypes[i].GetSize();
            }

            for (int i = 0; i < this.maxRowCount; i++)
            {
                if (BitArray.IsSet(i, this.storage.Span))
                {
                    ushort position = (ushort)(i * this.rowSize + this.dataStartPosition);

                    fixed (byte* ptr = this.storage.Span)
                    {
                        if ((*(T*)(ptr + position + colPosition)).CompareTo(elem) == 0)
                        {
                            return true;
                        }
                    }
                }
            }

            return false;
        }

With this fix these are the new values:

|                                   Method | RowsInTableNumber |       Mean |    Error |   StdDev |
|----------------------------------------- |------------------ |-----------:|---------:|---------:|
| InsertIntoBTreeSingleIntColumnRandomData |             10000 |   106.5 ms |  1.70 ms |  1.74 ms |
| InsertIntoBTreeSingleIntColumnRandomData |             50000 |   669.5 ms | 12.04 ms | 11.26 ms |
| InsertIntoBTreeSingleIntColumnRandomData |            100000 | 1,650.9 ms | 31.48 ms | 38.66 ms |
| InsertIntoBTreeSingleIntColumnRandomData |            200000 | 3,622.4 ms | 42.50 ms | 39.76 ms |

which is ~2x improvement.

Given that here we are working with concrete types we don't have the luxury of comparing RowHolder. Instead we need to resolve each template.

Tried to optimize RowsetHolder::InsertOrdered to avoid RowHolder alloc. Got ~10%.

|                                   Method | RowsInTableNumber |        Mean |     Error |    StdDev |
|----------------------------------------- |------------------ |------------:|----------:|----------:|
| InsertIntoBTreeSingleIntColumnRandomData |             10000 |    90.98 ms |  1.795 ms |  3.000 ms |
| InsertIntoBTreeSingleIntColumnRandomData |             50000 |   556.63 ms |  6.904 ms |  5.391 ms |
| InsertIntoBTreeSingleIntColumnRandomData |            100000 | 1,379.85 ms | 27.219 ms | 39.036 ms |
| InsertIntoBTreeSingleIntColumnRandomData |            200000 | 3,208.92 ms | 36.132 ms | 32.030 ms |

Next big chunks are for non-leaf iter and nlogn for insert into tree logic. Idea would be to always try to keep the page compact, without free space between the pages.

Removed btree RowHolder fetch. Replaced with iteration and tuple fetch:

|                                   Method | RowsInTableNumber |        Mean |     Error |    StdDev |
|----------------------------------------- |------------------ |------------:|----------:|----------:|
| InsertIntoBTreeSingleIntColumnRandomData |             10000 |    69.08 ms |  0.896 ms |  0.839 ms |
| InsertIntoBTreeSingleIntColumnRandomData |             50000 |   402.94 ms |  1.798 ms |  1.502 ms |
| InsertIntoBTreeSingleIntColumnRandomData |            100000 |   893.23 ms |  8.578 ms |  7.163 ms |
| InsertIntoBTreeSingleIntColumnRandomData |            200000 | 2,063.97 ms | 31.334 ms | 26.166 ms |

Also removing 10000 rows from the measurement. Don't care about benchmarks that are <100ms.

We are now at 100k/s. 10x to go.