using System;
using System.Linq;

namespace PageManager
{
    public unsafe struct RowHolderFixed
    {
        public readonly byte[] Storage;
        public readonly short[] ColumnPosition;

        public RowHolderFixed(ColumnType[] columnTypes)
        {
            this.ColumnPosition = new short[columnTypes.Length];

            for (int i = 0; i < columnTypes.Length - 1; i++)
            {
                this.ColumnPosition[i + 1] = (byte)(this.ColumnPosition[i] + ColumnTypeSize.GetSize(columnTypes[i]));
            }

            int totalSize = this.ColumnPosition[columnTypes.Length - 1] + ColumnTypeSize.GetSize(columnTypes[columnTypes.Length - 1]);

            this.Storage = new byte[totalSize];
        }

        public RowHolderFixed(ColumnInfo[] columnTypes, byte[] byteArr)
        {
            this.ColumnPosition = new short[columnTypes.Length];

            for (int i = 0; i < columnTypes.Length - 1; i++)
            {
                this.ColumnPosition[i + 1] = (byte)(this.ColumnPosition[i] + columnTypes[i].GetSize());
            }

            this.Storage = byteArr;
        }

        public RowHolderFixed(ColumnInfo[] columnTypes)
        {
            ushort size = CalculateSizeNeeded(columnTypes);
            byte[] storage = new byte[size];

            this.ColumnPosition = new short[columnTypes.Length];

            for (int i = 0; i < columnTypes.Length - 1; i++)
            {
                this.ColumnPosition[i + 1] = (byte)(this.ColumnPosition[i] + columnTypes[i].GetSize());
            }

            this.Storage = storage;
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

        // TODO: This is all super slow.
        // one thing to think about is to use spans are return value.
        // and let caller parse it in a way it finds suitable.
        public char[] GetStringField(int col)
        {
            short colPos = this.ColumnPosition[col];

            short size = BitConverter.ToInt16(this.Storage, colPos);

            char[] ret = new char[size];

            for (int i = 0; i < size; i++)
            {
                ret[i] = (char)this.Storage[i + sizeof(short) + colPos];
            }

            return ret;
        }

        public void SetField<T>(int col, T val) where T : unmanaged
        {
            fixed (byte* ptr = this.Storage)
            {
                *(T*)(ptr + ColumnPosition[col]) = val;
            }
        }

        public void SetField(int col, char[] val)
        {
            short colPos = this.ColumnPosition[col];
            byte[] length = BitConverter.GetBytes((ushort)val.Length);

            this.Storage[colPos] = length[0];
            this.Storage[colPos + 1] = length[1];

            for (int i = 0; i < val.Length; i++)
            {
                this.Storage[colPos + i + sizeof(short)] = (byte)val[i];
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

        public static ushort CalculateSizeNeeded(ColumnInfo[] columnInfos)
        {
            ushort sum = 0;
            foreach (ColumnInfo ci in columnInfos)
            {
                sum += ci.GetSize();
            }

            return sum;
        }
    }
}
