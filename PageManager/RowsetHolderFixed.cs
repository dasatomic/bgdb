using PageManager.UtilStructures;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace PageManager
{
    /// <summary>
    /// Structure:
    /// | bitmask | tuple beginnings | data |
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    unsafe public struct RowsetHolderFixed
    {
        private const int StorageSize = 4096;
        private fixed byte storage[StorageSize];
        private ushort rowSize;

        /// <summary>
        /// Reserved number of bytes for bitmask.
        /// </summary>
        private ushort reservedPresenceBitmaskCount;

        /// <summary>
        /// Reserved number of bytes for information about beginnings of individual rows
        /// in a tuple.
        /// </summary>
        private ushort reservedColumnTupleOffsetsCount;

        private ushort dataStartPosition;

        public RowsetHolderFixed(ColumnType[] columnTypes)
        {
            this.rowSize = GetRowSize(columnTypes);

            // bit for every row.
            this.reservedPresenceBitmaskCount = (ushort)(StorageSize / (rowSize * 8));

            // TODO: In this implementation each value in tuple can't be bigger than 256 bytes.
            // Since this is only for fixed data this should be fine.
            this.reservedColumnTupleOffsetsCount = (ushort)columnTypes.Length;

            int pos = this.reservedPresenceBitmaskCount;
            this.storage[pos] = 0;
            for (int i = 0; i < columnTypes.Length - 1; i++)
            {
                // TODO: need to check the offsets.
                this.storage[pos + 1] = (byte)(this.storage[pos] + (byte)ColumnTypeSize.GetSize(columnTypes[i]));
                pos++;
            }

            int dataStartUnAligned = this.reservedPresenceBitmaskCount + this.reservedColumnTupleOffsetsCount;
            this.dataStartPosition = (ushort)(((dataStartUnAligned + 4 - 1) / 4) * 4);
        }

        public T GetRowGeneric<T>(int row, int col) where T : unmanaged
        {
            System.Diagnostics.Debug.Assert(IsPresent(row));

            fixed (byte* ptr = this.storage)
            {
                return *(T*)(ptr + GetTuplePosition(row, col));
            }
        }

        public void SetRowGeneric<T>(int row, int col, T val) where T : unmanaged
        {
            fixed (byte* ptr = this.storage)
            {
                BitArray.Set(row, ptr);
                *(T*)(ptr + GetTuplePosition(row, col)) = val;
            }
        }


        // Private fields.

        private byte GetPositionInTuple(int col) =>
            this.storage[this.reservedPresenceBitmaskCount + col];

        private bool IsPresent(int row)
        {
            fixed (byte* ptr = this.storage)
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
