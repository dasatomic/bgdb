using System;

namespace PageManager
{
    public enum ColumnType
    {
        Int = 0,
        Double = 1,
        StringPointer = 2,
        PagePointer = 3,
        MaxColumnType = 4,
    }

    public static class ColumnTypeSize
    {
        public static ushort GetSize(ColumnType ct)
        {
            return ct switch
            {
                ColumnType.Double => sizeof(double),
                ColumnType.Int => sizeof(int),
                ColumnType.PagePointer => (ushort)PagePointerOffsetPair.Size,
                ColumnType.StringPointer => sizeof(long),
                _ => throw new ArgumentException()
            };
        }
    }

}
