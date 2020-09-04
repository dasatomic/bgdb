using PageManager.UtilStructures;
using System;
using System.Collections.Generic;
using System.Linq;
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
    public unsafe struct RowsetHolderFixed
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

        public RowsetHolderFixed(ColumnType[] columnTypes, Memory<byte> storage, bool init)
        {
            System.Diagnostics.Debug.Assert(BitConverter.IsLittleEndian, "Rowset holder fixed assumes that we are running on little endian");

            this.rowSize = GetRowSize(columnTypes);
            this.storage = storage;
            this.rowCount = 0;

            // bit for every row.
            this.reservedPresenceBitmaskCount = (ushort)(storage.Length / (rowSize * 8));

            // TODO: In this implementation each value in tuple can't be bigger than 256 bytes.
            // Since this is only for fixed data this should be fine.
            this.reservedColumnTupleOffsetsCount = (ushort)columnTypes.Length;

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
                    this.storage.Span[pos + 1] = (byte)(this.storage.Span[pos] + (byte)ColumnTypeSize.GetSize(columnTypes[i]));
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

            // Find current number of items.
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

        public void GetRow(int row, ref RowHolderFixed rowHolder)
        {
            System.Diagnostics.Debug.Assert(IsPresent(row));

            ushort position = (ushort)(row * this.rowSize + this.dataStartPosition);
            rowHolder.Fill(new Span<byte>(Unsafe.AsPointer(ref this.storage.Span[position]), this.rowSize));
        }

        public void SetRow(int row, RowHolderFixed rowHolder)
        {
            ushort position = (ushort)(row * this.rowSize + this.dataStartPosition);

            fixed (byte* ptr = this.storage.Span)
            {
                BitArray.Set(row, ptr);
                Marshal.Copy(rowHolder.Storage, 0, (IntPtr)(ptr + position), rowHolder.Storage.Length);
            }
        }

        public int InsertRow(RowHolderFixed rowHolder)
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

        public void DeleteRow(int position)
        {
            BitArray.Unset(position, this.storage.Span);
            this.rowCount--;
        }

        // TODO: This is not performant and it is not natural to pass column type here.
        public IEnumerable<RowHolderFixed> Iterate(ColumnType[] columnTypes)
        {
            for (int i = 0; i < this.maxRowCount; i++)
            {
                if (BitArray.IsSet(i, this.storage.Span))
                {
                    RowHolderFixed rowHolder = new RowHolderFixed(columnTypes);
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
            if (obj is RowsetHolderFixed)
            {
                RowsetHolderFixed c = (RowsetHolderFixed)obj;

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

        private static ushort GetRowSize(ColumnType[] columnTypes)
        {
            ushort size = 0;
            foreach (ColumnType ct in columnTypes)
            {
                size += ColumnTypeSize.GetSize(ct);
            }

            return size;
        }

        private int GetTuplePosition(int row, int col)
        {
            int tuplePosition = row * this.rowSize + this.dataStartPosition;
            int offsetInTouple = GetPositionInTuple(col);

            return tuplePosition + offsetInTouple;
        }
    }
}
