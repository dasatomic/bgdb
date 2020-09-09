using PageManager.UtilStructures;
using System;

namespace PageManager.PageTypes
{
    /// <summary>
    /// Internal class to Page Manager.
    /// Used for efficient bit packing.
    // This is just a wrapper around MixedPage with a single int column.
    /// </summary>
    class BitTrackingPage
    {
        private readonly MixedPage storage;
        private static readonly ColumnType[] columnTypes = new ColumnType[] { ColumnType.Int };

        public BitTrackingPage(MixedPage page)
        {
            this.storage = page;
        }

        public bool At(int pos, ITransaction transaction)
        {
            if (pos >= this.MaxItemCount() || pos < 0)
            {
                throw new ArgumentException();
            }

            (int positionInIntArray, int offsetInIntArray) = (pos / (sizeof(int) * 8), pos % (sizeof(int) * 8));
            RowHolderFixed rhf = new RowHolderFixed(columnTypes);
            storage.At((ushort)positionInIntArray, transaction, ref rhf);

            return BitArray.IsSet(offsetInIntArray, BitConverter.GetBytes(rhf.GetField<int>(0)));
        }

        public void Set(int pos, ITransaction transaction)
        {
            if (pos >= this.MaxItemCount() || pos < 0)
            {
                throw new ArgumentException();
            }

            (int positionInIntArray, int offsetInIntArray) = (pos / (sizeof(int) * 8), pos % (sizeof(int) * 8));
            RowHolderFixed rhf = new RowHolderFixed(columnTypes);
            storage.At((ushort)positionInIntArray, transaction, ref rhf);

            byte[] val = BitConverter.GetBytes(rhf.GetField<int>(0));
            BitArray.Set(offsetInIntArray, val);
            int updatedVal = BitConverter.ToInt32(val);

            rhf.SetField<int>(0, updatedVal);

            storage.Update(rhf, (ushort)positionInIntArray, transaction);
        }

        public int MaxItemCount() => (int)(this.storage.MaxRowCount() - 1) * sizeof(int) * 8;
    }
}
