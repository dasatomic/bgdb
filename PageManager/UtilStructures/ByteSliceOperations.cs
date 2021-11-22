using System;

namespace PageManager.UtilStructures
{
    public static class ByteSliceOperations
    {
        public static void ShiftSlice<T>(Memory<T> memory, int sourceStart, int destination, int elemCount)
        {
            Memory<T> sourceSlice = memory.Slice(sourceStart, elemCount);
            sourceSlice.CopyTo(memory.Slice(destination));
        }
    }
}
