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

        public static unsafe int FindUnset(byte* storage, int max)
        {
            for (int i = 0; i < max / 8; i++)
            {
                if (storage[i] != byte.MaxValue)
                {
                    for (int p = 0; p < 8; p++)
                    {
                        if ((storage[i] & 1 << p) == 0)
                        {
                            return i * 8 + p;
                        }

                    }
                }
            }

            // last chunk check.
            int lastByte = (max % 8);
            if (lastByte != 0)
            {
                for (int p = 0; p < lastByte; p++)
                {
                    if ((storage[max / 8] & 1 << p) == 0)
                    {
                        return ((max / 8) * 8 + p);
                    }
                }
            }

            return -1;
        }
    }
}
