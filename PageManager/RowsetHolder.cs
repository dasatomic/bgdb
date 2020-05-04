using System;
using System.Linq;

namespace PageManager
{
    public interface IRowsetHolder
    {
        public int[] GetIntColumn(int columnId);
        public double[] GetDoubleColumn(int columnId);
        public  PagePointerOffsetPair[] GetStringPointerColumn(int columnId);
        public  long[] GetPagePointerColumn(int columnId);
        public void SetColumns(int[][] intColumns, double[][] doubleColumns, PagePointerOffsetPair[][] pagePointerOffsetColumns, long[][] pagePointerColumns);
        public uint StorageSizeInBytes();
        public byte[] Serialize();
        public void SerializeInto(Span<byte> content);
        public void Deserialize(ReadOnlySpan<byte> bytes);
        public uint GetRowCount();
        public void Merge(RowsetHolder rowsetHolder);
    }

    public class RowsetHolder : IRowsetHolder
    {
        private int[][] intColumns;
        private PagePointerOffsetPair[][] pagePointerOffsetColumns;
        private double[][] doubleColumns;
        private long[][] pagePointerColumns;
        private int[] columnIdToTypeIdMappers;
        private uint rowsetCount = 0;

        public RowsetHolder(ColumnType[] columnTypes)
        {
            int intCount = 0;
            int doubleCount = 0;
            int pagePointerOffsetCount = 0;
            int pagePointerCount = 0;

            columnIdToTypeIdMappers = new int[columnTypes.Length];

            for (int i = 0; i < columnTypes.Length; i++)
            {
                switch (columnTypes[i])
                {
                    case ColumnType.Int:
                        columnIdToTypeIdMappers[i] = intCount;
                        intCount++;
                        break;
                    case ColumnType.Double:
                        columnIdToTypeIdMappers[i] = doubleCount;
                        doubleCount++;
                        break;
                    case ColumnType.StringPointer:
                        columnIdToTypeIdMappers[i] = pagePointerOffsetCount;
                        pagePointerOffsetCount++;
                        break;
                    case ColumnType.PagePointer:
                        columnIdToTypeIdMappers[i] = pagePointerCount;
                        pagePointerCount++;
                        break;
                    default:
                        throw new UnexpectedEnumValueException<ColumnType>(columnTypes[i]);
                }
            }

            this.intColumns = new int[intCount][];
            this.pagePointerOffsetColumns = new PagePointerOffsetPair[pagePointerOffsetCount][];
            this.doubleColumns = new double[doubleCount][];
            this.pagePointerColumns = new long[pagePointerCount][];
            this.rowsetCount = 0;
        }

        public uint GetRowCount() => this.rowsetCount;

        public int[] GetIntColumn(int columnId)
        {
            return intColumns[columnIdToTypeIdMappers[columnId]];
        }

        public double[] GetDoubleColumn(int columnId)
        {
            return doubleColumns[columnIdToTypeIdMappers[columnId]];
        }

        public PagePointerOffsetPair[] GetStringPointerColumn(int columnId)
        {
            return pagePointerOffsetColumns[columnIdToTypeIdMappers[columnId]];
        }

        private uint VerifyColumnValidityAndGetRowCount(int[][] intColumns, double[][] doubleColumns, PagePointerOffsetPair[][] pagePointerOffsetColumns, long[][] pagePointerColumns)
        {
            if (intColumns.Length != this.intColumns.Length ||
                doubleColumns.Length != this.doubleColumns.Length ||
                pagePointerColumns.Length != this.pagePointerColumns.Length ||
                pagePointerOffsetColumns.Length != this.pagePointerOffsetColumns.Length)
            {
                throw new InvalidRowsetDefinitionException();
            }

            int rowCount = 0;
            foreach (var intColum in intColumns)
            {
                if (rowCount == 0)
                {
                    rowCount = intColum.Length;
                }

                if (intColum.Length != rowCount)
                {
                    throw new InvalidRowsetDefinitionException();
                }
            }

            foreach (var doubleColum in doubleColumns)
            {
                if (rowCount == 0)
                {
                    rowCount = doubleColum.Length;
                }

                if (doubleColum.Length != rowCount)
                {
                    throw new InvalidRowsetDefinitionException();
                }
            }

            foreach (var pagePointerColumn in pagePointerColumns)
            {
                if (rowCount == 0)
                {
                    rowCount = pagePointerColumn.Length;
                }

                if (pagePointerColumn.Length != rowCount)
                {
                    throw new InvalidRowsetDefinitionException();
                }
            }

            foreach (var pagePointerOffsetColumn in pagePointerOffsetColumns)
            {
                if (rowCount == 0)
                {
                    rowCount = pagePointerOffsetColumn.Length;
                }

                if (pagePointerOffsetColumn.Length != rowCount)
                {
                    throw new InvalidRowsetDefinitionException();
                }
            }

            return (uint)rowCount;
        }

        public void SetColumns(int[][] intColumns, double[][] doubleColumns, PagePointerOffsetPair[][] pagePointerOffsetColumns, long[][] pagePointerColumns)
        {
            this.rowsetCount = this.VerifyColumnValidityAndGetRowCount(intColumns, doubleColumns, pagePointerOffsetColumns, pagePointerColumns);

            for (int i = 0; i < pagePointerOffsetColumns.Length; i++)
            {
                this.pagePointerOffsetColumns[i] = pagePointerOffsetColumns[i];
            }

            for (int i = 0; i < intColumns.Length; i++)
            {
                this.intColumns[i] = intColumns[i];
            }

            for (int i = 0; i < doubleColumns.Length; i++)
            {
                this.doubleColumns[i] = doubleColumns[i];
            }

            for (int i = 0; i < pagePointerColumns.Length; i++)
            {
                this.pagePointerColumns[i] = pagePointerColumns[i];
            }
        }

        public uint StorageSizeInBytes()
        {
            return sizeof(int) + this.rowsetCount * (uint)(PagePointerOffsetPair.Size * this.pagePointerOffsetColumns.Length +
                sizeof(int) * this.intColumns.Length +
                sizeof(double) * this.doubleColumns.Length +
                sizeof(long) * this.pagePointerColumns.Length);
        }

        public void SerializeInto(Span<byte> content)
        {
            if (content.Length < this.StorageSizeInBytes())
            {
                throw new InvalidRowsetDefinitionException();
            }

            bool success = BitConverter.TryWriteBytes(content, this.rowsetCount);
            System.Diagnostics.Debug.Assert(success);
            int currentPosition = sizeof(int);

            foreach (int[] iCol in this.intColumns)
            {
                foreach (int iVal in iCol)
                {
                    success = BitConverter.TryWriteBytes(content.Slice(currentPosition, sizeof(int)), iVal);
                    System.Diagnostics.Debug.Assert(success);
                    currentPosition += sizeof(int);
                }
            }

            foreach (double[] dCol in this.doubleColumns)
            {
                foreach (double dVal in dCol)
                {
                    success = BitConverter.TryWriteBytes(content.Slice(currentPosition, sizeof(double)), dVal);
                    System.Diagnostics.Debug.Assert(success);
                    currentPosition += sizeof(double);
                }
            }

            foreach (long[] lCol in this.pagePointerColumns)
            {
                foreach (long lVal in lCol)
                {
                    success = BitConverter.TryWriteBytes(content.Slice(currentPosition, sizeof(long)), lVal);
                    System.Diagnostics.Debug.Assert(success);
                    currentPosition += sizeof(long);
                }
            }

            foreach (PagePointerOffsetPair[] ppCol in this.pagePointerOffsetColumns)
            {
                foreach (PagePointerOffsetPair ppVal in ppCol)
                {
                    success = BitConverter.TryWriteBytes(content.Slice(currentPosition, sizeof(int)), ppVal.OffsetInPage);
                    System.Diagnostics.Debug.Assert(success);
                    currentPosition += sizeof(int);
                    success = BitConverter.TryWriteBytes(content.Slice(currentPosition, sizeof(long)), ppVal.PageId);
                    System.Diagnostics.Debug.Assert(success);
                    currentPosition += sizeof(long);
                }
            }
        }

        public byte[] Serialize()
        {
            uint sizeNeeded = StorageSizeInBytes();
            byte[] content = new byte[sizeNeeded];
            SerializeInto(content);

            return content;
        }

        public void Deserialize(ReadOnlySpan<byte> bytes)
        {
            this.rowsetCount = BitConverter.ToUInt32(bytes);

            if (bytes.Length != this.StorageSizeInBytes())
            {
                throw new InvalidRowsetDefinitionException();
            }

            // TODO: Use memory stream for this.
            int currentPosition = sizeof(int);
            for (int i = 0; i < intColumns.Length; i++)
            {
                intColumns[i] = new int[this.rowsetCount];

                for (int j = 0; j < rowsetCount; j++)
                {
                    intColumns[i][j] = BitConverter.ToInt32(bytes.Slice(currentPosition));
                    currentPosition += sizeof(int);
                }
            }

            for (int i = 0; i < doubleColumns.Length; i++)
            {
                doubleColumns[i] = new double[this.rowsetCount];

                for (int j = 0; j < rowsetCount; j++)
                {
                    doubleColumns[i][j] = BitConverter.ToDouble(bytes.Slice(currentPosition));
                    currentPosition += sizeof(double);
                }
            }

            for (int i = 0; i < pagePointerColumns.Length; i++)
            {
                pagePointerColumns[i] = new long[this.rowsetCount];

                for (int j = 0; j < rowsetCount; j++)
                {
                    pagePointerColumns[i][j] = BitConverter.ToInt64(bytes.Slice(currentPosition));
                    currentPosition += sizeof(long);
                }
            }

            for (int i = 0; i < pagePointerOffsetColumns.Length; i++)
            {
                pagePointerOffsetColumns[i] = new PagePointerOffsetPair[this.rowsetCount];

                for (int j = 0; j < rowsetCount; j++)
                {
                    pagePointerOffsetColumns[i][j].OffsetInPage = BitConverter.ToInt32(bytes.Slice(currentPosition));
                    currentPosition += sizeof(int);
                    pagePointerOffsetColumns[i][j].PageId = BitConverter.ToInt64(bytes.Slice(currentPosition));
                    currentPosition += sizeof(long);
                }
            }
        }

        public static uint CalculateSizeOfRow(ColumnType[] types)
        {
            int totalSize = 0;

            foreach (ColumnType type in types)
            {
                switch (type)
                {
                    case ColumnType.Int: totalSize += sizeof(int); break;
                    case ColumnType.Double: totalSize += sizeof(double); break;
                    case ColumnType.StringPointer: totalSize += (int)PagePointerOffsetPair.Size; break;
                    case ColumnType.PagePointer: totalSize += sizeof(long); break;
                    default:
                        throw new UnexpectedEnumValueException<ColumnType>(type);
                }
            }

            return (uint)totalSize;
        }

        public long[] GetPagePointerColumn(int columnId)
        {
            return pagePointerColumns[columnIdToTypeIdMappers[columnId]];
        }

        public void Merge(RowsetHolder rowsetHolder)
        {
            if (this.columnIdToTypeIdMappers.Length != rowsetHolder.columnIdToTypeIdMappers.Length)
            {
                throw new ArgumentException();
            }

            for (int i = 0; i < this.columnIdToTypeIdMappers.Length; i++)
            {
                if (this.columnIdToTypeIdMappers[i] != rowsetHolder.columnIdToTypeIdMappers[i])
                {
                    throw new ArgumentException();
                }
            }

            for (int i = 0; i < this.pagePointerOffsetColumns.Length; i++)
            {
                this.pagePointerOffsetColumns[i] = this.pagePointerOffsetColumns[i].Concat(rowsetHolder.pagePointerOffsetColumns[i]).ToArray();
            }

            for (int i = 0; i < this.intColumns.Length; i++)
            {
                this.intColumns[i] = this.intColumns[i].Concat(rowsetHolder.intColumns[i]).ToArray();
            }

            for (int i = 0; i < this.doubleColumns.Length; i++)
            {
                this.doubleColumns[i] = this.doubleColumns[i].Concat(rowsetHolder.doubleColumns[i]).ToArray();
            }

            for (int i = 0; i < this.pagePointerColumns.Length; i++)
            {
                this.pagePointerColumns[i] = this.pagePointerColumns[i].Concat(rowsetHolder.pagePointerColumns[i]).ToArray();
            }

            this.rowsetCount = this.rowsetCount + rowsetHolder.rowsetCount;
        }
    }
}
