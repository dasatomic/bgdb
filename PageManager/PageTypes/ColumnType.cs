using System;

namespace PageManager
{
    public enum ColumnType
    {
        Int = 0,
        Double = 1,
        // String of variable length are stored
        // in separate pages.
        StringPointer = 2,
        PagePointer = 3,
        // String for now is only ASCII, single byte encoding.
        // String is meant for columns of fixed length.
        String = 4,

        /// <summary>
        /// Maximal value. Update as new once are added.
        /// </summary>
        MaxColumnType = 5,
    }

    public struct ColumnInfo
    {
        public readonly ColumnType ColumnType;
        public readonly int RepCount;

        public ColumnInfo(ColumnType ct, int repCount)
        {
            this.ColumnType = ct;
            this.RepCount = repCount;
        }

        public ColumnInfo(ColumnType ct)
        {
            this.ColumnType = ct;
            this.RepCount = 0;
        }

        public ushort GetSize()
        {
            return this.ColumnType switch
            {
                ColumnType.Double => sizeof(double),
                ColumnType.Int => sizeof(int),
                ColumnType.StringPointer => (ushort)PagePointerOffsetPair.Size,
                ColumnType.PagePointer => sizeof(long),
                ColumnType.String => (ushort)(sizeof(char) * this.RepCount + sizeof(ushort)),
                _ => throw new ArgumentException()
            };
        }
    }

    public static class ColumnTypeSize
    {
        public static ushort GetSize(ColumnType ct)
        {
            return ct switch
            {
                ColumnType.Double => sizeof(double),
                ColumnType.Int => sizeof(int),
                ColumnType.StringPointer => (ushort)PagePointerOffsetPair.Size,
                ColumnType.PagePointer => sizeof(long),
                ColumnType.String => throw new NotSupportedException(),
                _ => throw new ArgumentException()
            };
        }
    }
}
