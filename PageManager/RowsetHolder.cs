using PageManager.UtilStructures;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace PageManager
{
    /// <summary>
    /// Structure:
    /// | page data | bitmask | tuple beginnings | data |
    /// Page is marked as unsafe struct which initializes it self
    /// directly from loaded memory (i.e. from buffer pool which just loads raw bytes from disk.
    /// It doesn't care about Endianess and presumes that data is loaded in the same format as it was stored.
    /// </summary>
    public unsafe struct RowsetHolder
    {
        private Memory<byte> storage;
        private readonly ushort rowSize;

        /// <summary>
        /// Reserved number of bytes for bitmask.
        /// </summary>
        private readonly ushort reservedPresenceBitmaskCount;

        /// <summary>
        /// Reserved number of bytes for information about beginnings of individual rows
        /// in a tuple.
        /// </summary>
        private readonly ushort reservedColumnTupleOffsetsCount;

        private readonly ushort dataStartPosition;

        private readonly ushort maxRowCount;

        private ushort rowCount;

        public RowsetHolder(ColumnInfo[] columnTypes, Memory<byte> storage, bool init)
        {
            System.Diagnostics.Debug.Assert(BitConverter.IsLittleEndian, "Rowset holder fixed assumes that we are running on little endian");

            this.rowSize = GetRowSize(columnTypes);
            this.storage = storage;
            this.rowCount = 0;

            // bit for every row.
            // align on upper boundary.
            this.reservedPresenceBitmaskCount = (ushort)UtilStructures.IntCeil.CeilDiv(storage.Length, (rowSize * 8));

            // TODO: In this implementation each value in tuple can't be bigger than 256 bytes.
            // Since this is only for fixed data this should be fine.
            this.reservedColumnTupleOffsetsCount = (ushort)(columnTypes.Length * sizeof(ushort));

            if (init)
            {
                // set bitmask to 0.
                for (int i = 0; i < this.reservedPresenceBitmaskCount; i++)
                {
                    this.storage.Span[i] = 0;
                }

                int pos = this.reservedPresenceBitmaskCount;
                this.storage.Span[pos] = 0;
                for (int i = 0; i < columnTypes.Length - 1; i++)
                {
                    this.storage.Span[pos + 1] = (byte)(this.storage.Span[pos] + columnTypes[i].GetSize());
                    pos++;
                }
            }
            else
            {
                // Set row count.
                this.rowCount = BitArray.CountSet(storage.Span.Slice(0, this.reservedPresenceBitmaskCount));
            }

            // Align so start is divisible by 4.
            // TODO: Need to measure perf.
            int dataStartUnAligned = this.reservedPresenceBitmaskCount + this.reservedColumnTupleOffsetsCount;
            this.dataStartPosition = (ushort)(((dataStartUnAligned + 4 - 1) / 4) * 4);

            maxRowCount = (ushort)((storage.Length - dataStartPosition) / rowSize);
        }

        public void UpdateRowCount()
        {
            this.rowCount = BitArray.CountSet(storage.Span.Slice(0, this.reservedPresenceBitmaskCount));
        }

        public T GetRowGeneric<T>(int row, int col) where T : unmanaged
        {
            System.Diagnostics.Debug.Assert(IsPresent(row));

            fixed (byte* ptr = this.storage.Span)
            {
                return *(T*)(ptr + GetTuplePosition(row, col));
            }
        }

        public void SetRowGeneric<T>(int row, int col, T val) where T : unmanaged
        {
            fixed (byte* ptr = this.storage.Span)
            {
                BitArray.Set(row, ptr);
                *(T*)(ptr + GetTuplePosition(row, col)) = val;
            }
        }

        public void GetRow(int row, ref RowHolder rowHolder)
        {
            System.Diagnostics.Debug.Assert(IsPresent(row));

            ushort position = (ushort)(row * this.rowSize + this.dataStartPosition);
            rowHolder.Fill(new Span<byte>(Unsafe.AsPointer(ref this.storage.Span[position]), this.rowSize));
        }

        public void SetRow(int row, RowHolder rowHolder)
        {
            ushort position = (ushort)(row * this.rowSize + this.dataStartPosition);

            fixed (byte* ptr = this.storage.Span)
            {
                BitArray.Set(row, ptr);
                Marshal.Copy(rowHolder.Storage, 0, (IntPtr)(ptr + position), rowHolder.Storage.Length);
            }
        }

        public int InsertRow(RowHolder rowHolder)
        {
            fixed (byte* ptr = this.storage.Span)
            {
                int emptyPosition = BitArray.FindUnset(ptr, this.maxRowCount);

                if (emptyPosition == -1)
                {
                    return -1;
                }

                this.SetRow(emptyPosition, rowHolder);
                this.rowCount++;

                return emptyPosition;
            }
        }

        public int InsertRowOrdered(RowHolder rowHolderToInsert, ColumnInfo[] columnTypes, Func<RowHolder, RowHolder, int> comparer)
        {
            // find the first element that is bigger than one to insert.
            // TODO: this can be logn.
            int positionToInsert = -1;
            for (int i = 0; i < this.maxRowCount; i++)
            {
                if (BitArray.IsSet(i, this.storage.Span))
                {
                    RowHolder rowHolder = new RowHolder(columnTypes);
                    GetRow(i, ref rowHolder);
                    if (comparer(rowHolderToInsert, rowHolder) != 1)
                    {
                        // I am smaller than you, I should be at your place.
                        positionToInsert = i;
                        break;
                    } else
                    {
                        // Bigger than everyone else. Go at the end.
                        positionToInsert = -1;
                    }
                }
                else 
                {
                    if (positionToInsert == -1)
                    {
                        positionToInsert = i;
                    }
                }
            }

            if (positionToInsert == -1)
            {
                // insert at the end.
                positionToInsert = this.maxRowCount - 1;
            }

            if (BitArray.IsSet(positionToInsert, this.storage.Span))
            {
                int firstFreeElement = -1;
                // need to shift everything.
                // Try first to find free element on the right.
                for (int i = positionToInsert + 1; i < this.maxRowCount; i++)
                {
                    if (!BitArray.IsSet(i, this.storage.Span))
                    {
                        firstFreeElement = i;
                        break;
                    }
                }

                if (firstFreeElement == -1)
                {
                    // Search on the left.
                    for (int i = positionToInsert - 1; i >= 0; i--)
                    {
                        if (!BitArray.IsSet(i, this.storage.Span))
                        {
                            firstFreeElement = i;
                            break;
                        }
                    }

                    if (firstFreeElement == -1)
                    {
                        // No free space.
                        return -1;
                    }
                }

                int numOfElemToCopy = Math.Abs(positionToInsert - firstFreeElement);

                if (positionToInsert < firstFreeElement)
                {
                    // shift right one element.
                    ByteSliceOperations.ShiftSlice<byte>(
                        this.storage,
                        this.dataStartPosition + positionToInsert * this.rowSize, // Source.
                        this.dataStartPosition + (positionToInsert + 1) * this.rowSize, // Destination.
                        numOfElemToCopy * this.rowSize);
                }
                else
                {
                    // shift left.
                    ByteSliceOperations.ShiftSlice<byte>(
                        this.storage,
                        this.dataStartPosition + (firstFreeElement + 1) * this.rowSize, // Source.
                        this.dataStartPosition + firstFreeElement * this.rowSize, // Destination.
                        numOfElemToCopy * this.rowSize);
                }

                BitArray.Set(firstFreeElement, this.storage.Span);
            }

            this.SetRow(positionToInsert, rowHolderToInsert);
            BitArray.Set(positionToInsert, this.storage.Span);
            this.rowCount++;
            return positionToInsert;
        }

        public void DeleteRow(int position)
        {
            BitArray.Unset(position, this.storage.Span);
            this.rowCount--;
        }

        /// <summary>
        /// Split in the middle.
        /// </summary>
        /// <returns>Returns right side of the split + split value.</returns>
        public void SplitPage(Memory<byte> newPage, ref RowHolder splitValue, int elemNumForSplit)
        {
            if (this.rowCount % 2 != 1)
            {
                throw new ArgumentException("Page needs to have uneven number of elements to make the split.");
            }

            this.storage.CopyTo(newPage);

            for (int i = 0; i < elemNumForSplit + 1; i++)
            {
                // Unset presence in the first half of new page.
                BitArray.Unset(i, newPage.Span);
            }

            this.GetRow(elemNumForSplit, ref splitValue);

            // Caller will have to refresh row count in this item with UpdateRowCount call.
            // we can't do it here since we are operating on row memory.
            for (int i = elemNumForSplit; i < this.maxRowCount; i++)
            {
                BitArray.Unset(i, this.storage.Span);
            }

            this.UpdateRowCount();
        }

        // TODO: This is not performant and it is not natural to pass column type here.
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

        public IEnumerable<RowHolder> IterateReverse(ColumnInfo[] columnTypes)
        {
            for (int i = this.maxRowCount - 1; i >= 0; i--)
            {
                if (BitArray.IsSet(i, this.storage.Span))
                {
                    RowHolder rowHolder = new RowHolder(columnTypes);
                    GetRow(i, ref rowHolder);
                    yield return rowHolder;
                }
            }
        }

        public ushort MaxRowCount() => this.maxRowCount;

        public int FreeSpaceForItems()
        {
            return this.maxRowCount - this.rowCount;
        }

        public int GetRowCount()
        {
            return this.rowCount;
        }

        public override bool Equals(object obj)
        {
            if (obj is RowsetHolder)
            {
                RowsetHolder c = (RowsetHolder)obj;

                if (this.storage.Length != c.storage.Length)
                {
                    return false;
                }

                for (int i = 0; i < this.storage.Length; i++)
                {
                    if (this.storage.Span[i] != c.storage.Span[i])
                    {
                        return false;
                    }
                }

                return true;
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            int i = this.storage.Length;
            int hc = i + 1;

            while (--i >= 0)
            {
                hc *= 257;
                hc ^= this.storage.Span[i];
            }

            return hc;
        }

        // Private fields.

        private byte GetPositionInTuple(int col) =>
            this.storage.Span[this.reservedPresenceBitmaskCount + col];

        private bool IsPresent(int row)
        {
            fixed (byte* ptr = this.storage.Span)
            {
                return BitArray.IsSet(row, ptr);
            }
        }

        private static ushort GetRowSize(ColumnInfo[] columnInfo)
        {
            ushort sum = 0;
            foreach (ColumnInfo ci in columnInfo)
            {
                sum += ci.GetSize();
            }

            return sum;
        }

        private int GetTuplePosition(int row, int col)
        {
            int tuplePosition = row * this.rowSize + this.dataStartPosition;
            int offsetInTouple = GetPositionInTuple(col);

            return tuplePosition + offsetInTouple;
        }
    }
}
