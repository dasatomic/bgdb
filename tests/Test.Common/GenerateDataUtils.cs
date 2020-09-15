using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Test.Common
{
    public static class GenerateDataUtils
    {
        public static List<ColumnInfo> GenerateRandomColumns(int colNum)
        {
            List<ColumnInfo> result = new List<ColumnInfo>();
            Random rnd = new Random();

            for (int i = 0; i < colNum; i++)
            {
                ColumnType ct = (ColumnType)rnd.Next(0, (int)ColumnType.MaxColumnType);
                if (ct == ColumnType.String)
                {
                    int strLength = rnd.Next(1, 20);
                    result.Add(new ColumnInfo(ct, strLength));
                }
                else
                {
                    result.Add(new ColumnInfo(ct));
                }
            }

            return result;
        }

        public static List<RowHolderFixed> GenerateRowsWithSampleData(out ColumnInfo[] columnTypes, int rowNumber = 10)
        {
            List<RowHolderFixed> rhfs = new List<RowHolderFixed>();

            columnTypes = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int),
                new ColumnInfo(ColumnType.Int),
                new ColumnInfo(ColumnType.Double),
                new ColumnInfo(ColumnType.Int),
                new ColumnInfo(ColumnType.StringPointer),
                new ColumnInfo(ColumnType.PagePointer),
            };

            for (int i = 0; i < 10; i++)
            {
                RowHolderFixed rowhf = new RowHolderFixed(columnTypes);
                rowhf.SetField<int>(0, i);
                rowhf.SetField<int>(1, i + 1);
                rowhf.SetField<double>(2, (double)i);
                rowhf.SetField<int>(3, i + 2);
                rowhf.SetField(4, new PagePointerOffsetPair(i, i));
                rowhf.SetField<ulong>(5, (ulong)i + 3);

                rhfs.Add(rowhf);
            }

            return rhfs;
        }

        public static void GenerateSampleData(
            out ColumnInfo[] types,
            out int[][] intColumns,
            out double[][] doubleColumns,
            out long[][] pagePointerColumns,
            out PagePointerOffsetPair[][] pagePointerOffsetColumns,
            int rowCount = 5)
        {
            types = new ColumnInfo[]
            {
                new ColumnInfo(ColumnType.Int),
                new ColumnInfo(ColumnType.Int),
                new ColumnInfo(ColumnType.Double),
                new ColumnInfo(ColumnType.Int),
                new ColumnInfo(ColumnType.StringPointer),
                new ColumnInfo(ColumnType.PagePointer),
            };

            int intColumnCount = types.Count(t => t.ColumnType == ColumnType.Int);
            int doubleColumnCount = types.Count(t => t.ColumnType == ColumnType.Double);
            int pagePointerOffsetCount = types.Count(t => t.ColumnType == ColumnType.StringPointer);
            int pagePointerCount = types.Count(t => t.ColumnType == ColumnType.PagePointer);

            intColumns = new int[intColumnCount][];
            for (int i = 0; i < intColumns.Length; i++)
            {
                intColumns[i] = Enumerable.Repeat(i, rowCount).ToArray();
            }

            doubleColumns = new double[doubleColumnCount][];
            for (int i = 0; i < doubleColumns.Length; i++)
            {
                doubleColumns[i] = new double[rowCount];

                for (int j = 0; j < rowCount; j++)
                {
                    doubleColumns[i][j] = (double)j;
                }
            }

            pagePointerColumns = new long[pagePointerCount][];
            for (int i = 0; i < pagePointerColumns.Length; i++)
            {
                pagePointerColumns[i] = new long[rowCount];

                for (int j = 0; j < rowCount; j++)
                {
                    pagePointerColumns[i][j] = (long)j;
                }
            }

            pagePointerOffsetColumns = new PagePointerOffsetPair[pagePointerOffsetCount][];
            for (int i = 0; i < pagePointerOffsetColumns.Length; i++)
            {
                pagePointerOffsetColumns[i] = new PagePointerOffsetPair[rowCount];

                for (int j = 0; j < rowCount; j++)
                {
                    pagePointerOffsetColumns[i][j].PageId = i;
                    pagePointerOffsetColumns[i][j].OffsetInPage = i;
                }
            }
        }
    }
}
