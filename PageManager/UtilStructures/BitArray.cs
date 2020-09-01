namespace PageManager.UtilStructures
{
    public static class BitArray
    {
        public static unsafe bool IsSet(int row, byte* storage)
        {
            return (storage[row / 8] & (byte)(1 << (row % 8))) != 0;
        }

        public static unsafe void Set(int row, byte* storage)
        {
            storage[row / 8] |= (byte)(1 << (row % 8));
        }

        public static unsafe void Unset(int row, byte* storage)
        {
            storage[row / 8] &= (byte)(~(1 << (row % 8)));
        }
    }
}
