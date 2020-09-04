using System;
using System.Linq;

namespace PageManager
{
    public unsafe struct RowHolderFixed
    {
        public readonly byte[] Storage;
        public readonly byte[] ColumnPosition;

        public RowHolderFixed(ColumnType[] columnTypes)
        {
            this.ColumnPosition = new byte[columnTypes.Length];

            for (int i = 0; i < columnTypes.Length - 1; i++)
            {
                this.ColumnPosition[i + 1] = (byte)(this.ColumnPosition[i] + ColumnTypeSize.GetSize(columnTypes[i]));
            }

            int totalSize = this.ColumnPosition[columnTypes.Length - 1] + ColumnTypeSize.GetSize(columnTypes[columnTypes.Length - 1]);

            this.Storage = new byte[totalSize];
        }

        public RowHolderFixed(ColumnType[] columnTypes, byte[] byteArr)
        {
            this.ColumnPosition = new byte[columnTypes.Length];

            for (int i = 0; i < columnTypes.Length - 1; i++)
            {
                this.ColumnPosition[i + 1] = (byte)(this.ColumnPosition[i] + ColumnTypeSize.GetSize(columnTypes[i]));
            }

            this.Storage = byteArr;
        }

        public void Fill(Span<byte> arr)
        {
            arr.CopyTo(this.Storage);
        }

        public T GetField<T>(int col) where T : unmanaged
        {
            fixed (byte* ptr = this.Storage)
            {
                return *(T*)(ptr + ColumnPosition[col]);
            }
        }

        public void SetField<T>(int col, T val) where T : unmanaged
        {
            fixed (byte* ptr = this.Storage)
            {
                *(T*)(ptr + ColumnPosition[col]) = val;
            }
        }

        public override bool Equals(object obj)
        {
            if (obj is RowHolderFixed)
            {
                RowHolderFixed c = (RowHolderFixed)obj;
                return Enumerable.SequenceEqual(c.Storage, this.Storage) && Enumerable.SequenceEqual(c.ColumnPosition, this.ColumnPosition);
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Storage, ColumnPosition);
        }
    }
}
