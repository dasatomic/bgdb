using System;
using System.Diagnostics;
using System.Linq;

namespace PageManager
{
    /// <summary>
    /// Arguments of project/extend request against row holder.
    /// Specifies columns that are to be copied from source row holder
    /// and ones that are to be initialized with default values.
    /// </summary>
    public struct ProjectExtendInfo
    {
        /// <summary>
        /// True if item on given position is to be projected. False otherwise.
        /// </summary>
        public bool[] IsProjection;

        /// <summary>
        /// Position in source rowholder for projection.
        /// To be applied only if isProjection[currPos] == true.
        /// </summary>
        public int[] ProjectSourcePositions;

        /// <summary>
        /// Extension info.
        /// To be applied only if isProjection[currPos] == false.
        /// </summary>
        public ColumnInfo[] ExtendColumnInfo;

        public ProjectExtendInfo(bool[] isProjection, int[] projectSourcePositions, ColumnInfo[] extendColumnInfo)
        {
            if (isProjection.Length != projectSourcePositions.Length + extendColumnInfo.Length)
            {
                throw new ArgumentException("All elements in is projection array need to be covered by either project source pos or extend column info.");
            }

            IsProjection = isProjection;
            ProjectSourcePositions = projectSourcePositions;
            ExtendColumnInfo = extendColumnInfo;
        }
    }

    public unsafe struct RowHolder
    {
        public readonly byte[] Storage;
        public readonly short[] ColumnPosition;

        public static RowHolder Zero() => new RowHolder(new byte[0], new short[0]);

        private RowHolder(byte[] storage, short[] columnPositions)
        {
            this.Storage = storage;
            this.ColumnPosition = columnPositions;
        }

        public RowHolder(ColumnType[] columnTypes)
        {
            this.ColumnPosition = new short[columnTypes.Length];

            for (int i = 0; i < columnTypes.Length - 1; i++)
            {
                this.ColumnPosition[i + 1] = (byte)(this.ColumnPosition[i] + ColumnTypeSize.GetSize(columnTypes[i]));
            }

            int totalSize = this.ColumnPosition[columnTypes.Length - 1] + ColumnTypeSize.GetSize(columnTypes[columnTypes.Length - 1]);

            this.Storage = new byte[totalSize];
        }

        public RowHolder(ColumnInfo[] columnTypes, byte[] byteArr)
        {
            this.ColumnPosition = new short[columnTypes.Length];

            for (int i = 0; i < columnTypes.Length - 1; i++)
            {
                this.ColumnPosition[i + 1] = (byte)(this.ColumnPosition[i] + columnTypes[i].GetSize());
            }

            this.Storage = byteArr;
        }

        public RowHolder(ColumnInfo[] columnTypes)
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

        private RowHolder(short[] columnPositions, byte[] data)
        {
            this.ColumnPosition = columnPositions;
            this.Storage = data;
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

            if (this.Storage.Length < colPos + val.Length + sizeof(short))
            {
                throw new ArgumentException("This val can't fit in this rowholder");
            }

            this.Storage[colPos] = length[0];
            this.Storage[colPos + 1] = length[1];

            for (int i = 0; i < val.Length; i++)
            {
                this.Storage[colPos + i + sizeof(short)] = (byte)val[i];
            }
        }

        public override bool Equals(object obj)
        {
            if (obj is RowHolder)
            {
                RowHolder c = (RowHolder)obj;
                return Enumerable.SequenceEqual(c.Storage, this.Storage) && Enumerable.SequenceEqual(c.ColumnPosition, this.ColumnPosition);
            }
            else
            {
                return false;
            }
        }

        public RowHolder Project(int[] cols)
        {
            // only copy relevant chunks of data.
            short[] newColPositions = new short[cols.Length];
            short totalSize = 0;
            newColPositions[0] = 0;
            for (int i = 0; i < cols.Length; i++)
            {
                short diff;
                if (cols[i] == this.ColumnPosition.Length - 1)
                {
                    diff = (short)(this.Storage.Length - this.ColumnPosition[cols[i]]);
                }
                else
                {
                    diff = (short)(this.ColumnPosition[cols[i] + 1] - this.ColumnPosition[cols[i]]);
                }

                totalSize += diff;

                if (i != cols.Length - 1)
                {
                    newColPositions[i + 1] = (short)(newColPositions[i] + diff);
                }
            }

            byte[] newStorage = new byte[totalSize];
            for (int i = 0; i < cols.Length; i++)
            {
                short sourceIndex = this.ColumnPosition[cols[i]];
                short sourceLenght;

                if (cols[i] == this.ColumnPosition.Length - 1)
                {
                    sourceLenght = (short)(this.Storage.Length - this.ColumnPosition[cols[i]]);
                }
                else
                {
                    sourceLenght = (short)(this.ColumnPosition[cols[i] + 1] - this.ColumnPosition[cols[i]]);
                }

                for (int j = 0; j < sourceLenght; j++)
                {
                    newStorage[newColPositions[i] + j] = this.Storage[sourceIndex + j];
                }
            }

            return new RowHolder(newColPositions, newStorage);
        }

        public RowHolder ProjectAndExtend(ProjectExtendInfo projectExtendInfo)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Each column in new RowHolder will be either projected from source RowHolder
        /// by using first element of tuple, or extended with new type by using given type and filled with default value.
        /// </summary>
        /// <param name="projectExtendInfo"></param>
        /// <returns></returns>
        public RowHolder ProjectAndExtend((int?, ColumnInfo?)[] projectExtendInfo)
        {
            short[] newColPositions = new short[projectExtendInfo.Length];
            short totalSize = 0;
            newColPositions[0] = 0;

            for (int i = 0; i < projectExtendInfo.Length; i++)
            {
                short diff;

                if (projectExtendInfo[i].Item1 != null)
                {
                    if (projectExtendInfo[i].Item1.Value == this.ColumnPosition.Length - 1)
                    {
                        diff = (short)(this.Storage.Length - this.ColumnPosition[projectExtendInfo[i].Item1.Value]);
                    }
                    else
                    {
                        diff = (short)(this.ColumnPosition[projectExtendInfo[i].Item1.Value + 1] - this.ColumnPosition[projectExtendInfo[i].Item1.Value]);
                    }
                }
                else
                {
                    Debug.Assert(projectExtendInfo[i].Item2 != null);

                    diff = (short)projectExtendInfo[i].Item2.Value.GetSize();
                }

                totalSize += diff;

                if (i != projectExtendInfo.Length - 1)
                {
                    newColPositions[i + 1] = (short)(newColPositions[i] + diff);
                }
            }

            byte[] newStorage = new byte[totalSize];
            for (int i = 0; i < projectExtendInfo.Length; i++)
            {
                if (projectExtendInfo[i].Item1 != null)
                {
                    // This is project operation. For extend we don't want to do anything.
                    short sourceIndex = this.ColumnPosition[projectExtendInfo[i].Item1.Value];
                    short sourceLength;

                    if (projectExtendInfo[i].Item1.Value == this.ColumnPosition.Length - 1)
                    {
                        sourceLength = (short)(this.Storage.Length - this.ColumnPosition[projectExtendInfo[i].Item1.Value]);
                    }
                    else
                    {
                        sourceLength = (short)(this.ColumnPosition[projectExtendInfo[i].Item1.Value + 1] - this.ColumnPosition[projectExtendInfo[i].Item1.Value]);
                    }

                    for (int j = 0; j < sourceLength; j++)
                    {
                        newStorage[newColPositions[i] + j] = this.Storage[sourceIndex + j];
                    }
                }
            }

            return new RowHolder(newColPositions, newStorage);
        }

        public RowHolder Merge(RowHolder that)
        {
            short[] newColPositions = new short[this.ColumnPosition.Length + that.ColumnPosition.Length];

            for (int i = 0; i < this.ColumnPosition.Length; i++)
            {
                newColPositions[i] = this.ColumnPosition[i];
            }

            for (int i = 0; i < that.ColumnPosition.Length; i++)
            {
                newColPositions[this.ColumnPosition.Length + i] = (short)(that.ColumnPosition[i] + this.Storage.Length);
            }

            byte[] newStorage = new byte[this.Storage.Length + that.Storage.Length];

            this.Storage.CopyTo(newStorage, 0);
            that.Storage.CopyTo(newStorage, this.Storage.Length);

            return new RowHolder(newColPositions, newStorage);
        }

        public override int GetHashCode()
        {
            HashCode hash = new HashCode();

            if (this.Storage == null)
            {
                // This is identity (empty) row.
                return 0;
            }

            foreach (byte b in this.Storage)
            {
                hash.Add(b);
            }

            return hash.ToHashCode();
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
