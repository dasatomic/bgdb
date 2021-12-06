using System;
using System.Collections.Generic;
using System.IO;

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
            if (ct != ColumnType.String && repCount != 0)
            {
                throw new ArgumentException();
            }

            this.ColumnType = ct;
            this.RepCount = repCount;
        }

        public ColumnInfo(ColumnType ct)
        {
            if (ct == ColumnType.String)
            {
                throw new ArgumentException();
            }

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

        public static ColumnInfo Deserialize(BinaryReader source)
        {
            ColumnType ct = (ColumnType)source.ReadSByte();
            if (IsVarLength(ct))
            {
                ushort len = source.ReadUInt16();
                return new ColumnInfo(ct, len);
            }
            else
            {
                return new ColumnInfo(ct);
            }
        }

        public void Serialize(BinaryWriter bw)
        {
            if (IsVarLength(this.ColumnType))
            {
                bw.Write((ushort)this.RepCount);
                bw.Write((sbyte)this.ColumnType);
            }
            else
            {
                bw.Write((sbyte)this.ColumnType);
            }
        }

        public static bool IsVarLength(ColumnType ct)
        {
            return ct switch
            {
                ColumnType.String => true,
                ColumnType.Double => false,
                ColumnType.PagePointer => false,
                ColumnType.Int => false,
                ColumnType.StringPointer => false,
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

    public interface ColumnTypeHandlerAllSingle<T>
    {
        T HandleInt();
        T HandleDouble();
        T HandleString();
        T HandleStringPointer();
        T HandlePagePointer();
    }

    public interface ColumnTypeHandlerBasicSingle<T>
    {
        T HandleInt();
        T HandleDouble();
        T HandleString();
    }

    public interface ColumnTypeHandlerAllDouble<T>
    {
        T HandleInt();
        T HandleDouble();
        T HandleString();
        T HandleStringPointer();
        T HandlePagePointer();
    }

    public interface ColumnTypeHandlerBasicDouble<T>
    {
        T HandleInt();
        T HandleDouble();
        T HandleString();
    }

    public interface ColumnTypeHandlerBasicScalar<T>
    {
        T HandleInt(int value);
        T HandleDouble(double value);
        T HandleString(string value);
    }

    public interface ColumnTypeHandlerBasicArray<T>
    {
        T HandleInt(IEnumerable<int> value);
        T HandleDouble(IEnumerable<double> value);
        T HandleString(IEnumerable<string> value);
    }

    public static class ColumnTypeHandlerRouter<T>
    {
        public static T Route(ColumnTypeHandlerBasicDouble<T> handler, ColumnType columnType)
        {
            switch (columnType)
            {
                case ColumnType.Int:
                    return handler.HandleInt();
                case ColumnType.Double:
                    return handler.HandleDouble();
                case ColumnType.String:
                    return handler.HandleString();
                default:
                    throw new NotImplementedException();
            }
        }

        public static T Route(ColumnTypeHandlerBasicSingle<T> handler, ColumnType columnType)
        {
            switch (columnType)
            {
                case ColumnType.Int:
                    return handler.HandleInt();
                case ColumnType.Double:
                    return handler.HandleDouble();
                case ColumnType.String:
                    return handler.HandleString();
                default:
                    throw new NotImplementedException();
            }
        }

        // TODO: Add other handlers when needed.
    }

    public struct CharrArray : IComparable<CharrArray>
    {
        public char[] Array;

        public CharrArray(char[] arr)
        {
            this.Array = arr;
        }

        public int CompareTo(CharrArray other)
        {
            for (int i = 0; i < this.Array.Length; i++)
            {
                if (i >= other.Array.Length)
                {
                    // I am bigger.
                    return 1;
                }

                if (this.Array[i] < other.Array[i])
                {
                    return -1;
                }

                if (this.Array[i] > other.Array[i])
                {
                    return 1;
                }
            }

            if (this.Array.Length < other.Array.Length)
            {
                // I am smaller.
                return -1;
            }

            return 0;
        }

        public int CompareToString(string other)
        {
            for (int i = 0; i < this.Array.Length; i++)
            {
                if (i >= other.Length)
                {
                    // I am bigger.
                    return 1;
                }

                if (this.Array[i] < other[i])
                {
                    return -1;
                }

                if (this.Array[i] > other[i])
                {
                    return 1;
                }
            }

            if (this.Array.Length < other.Length)
            {
                // I am smaller.
                return -1;
            }

            return 0;
        }

        public static int Compare(IEnumerable<char> str1, IEnumerable<char> str2)
        {
            using (IEnumerator<char> str1Iter = str1.GetEnumerator())
            using (IEnumerator<char> str2Iter = str2.GetEnumerator())
            {
                while (str1Iter.MoveNext() && str2Iter.MoveNext())
                {
                    if (str1Iter.Current < str2Iter.Current)
                    {
                        return -1;
                    }

                    if (str1Iter.Current > str2Iter.Current)
                    {
                        return 1;
                    }
                }

                if (str1Iter.MoveNext())
                {
                    // first one is longer, hance it is bigger.
                    return 1;
                }

                if (str2Iter.MoveNext())
                {
                    return -1;
                }

                return 0;
            }
        }
    }
}
