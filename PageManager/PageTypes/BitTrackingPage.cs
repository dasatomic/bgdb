using PageManager.UtilStructures;
using System;
using System.Reflection.Metadata.Ecma335;

namespace PageManager.PageTypes
{
    /// <summary>
    /// Used for efficient bit packing.
    // This is just a wrapper around MixedPage with a single int column.
    /// </summary>
    public class BitTrackingPage
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
            RowHolder rhf = new RowHolder(columnTypes);
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
            RowHolder rhf = new RowHolder(columnTypes);
            storage.At((ushort)positionInIntArray, transaction, ref rhf);

            byte[] val = BitConverter.GetBytes(rhf.GetField<int>(0));
            BitArray.Set(offsetInIntArray, val);
            int updatedVal = BitConverter.ToInt32(val);

            rhf.SetField<int>(0, updatedVal);

            storage.Update(rhf, (ushort)positionInIntArray, transaction);
        }

        public System.Collections.Generic.IEnumerable<int> FindAllSet(ITransaction tran)
        {
            for (int i = 0; i < this.MaxItemCount(); i++)
            {
                if (this.At(i, tran))
                {
                    yield return i;
                }
            }
        }

        public int MaxItemCount() => (int)(this.storage.MaxRowCount() - 1) * sizeof(int) * 8;

        public static void NullifyMixedPage(MixedPage page, ITransaction tran)
        {
            for (int i = 0; i < page.MaxRowCount(); i++)
            {
                RowHolder rhf = new RowHolder(new ColumnType[] { ColumnType.Int });
                rhf.SetField<int>(0, 0);
                page.Insert(rhf, tran);
            }
        }

        public MixedPage GetStoragePage() => this.storage;
    }
}
