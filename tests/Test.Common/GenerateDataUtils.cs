using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Test.Common
{
    public static class GenerateDataUtils
    {
        public static List<RowHolderFixed> GenerateRowsWithSampleData(out ColumnType[] columnTypes, int rowNumber = 10)
        {
            List<RowHolderFixed> rhfs = new List<RowHolderFixed>();

            columnTypes = new ColumnType[]
            {
                ColumnType.Int,
                ColumnType.Int,
                ColumnType.Double,
                ColumnType.Int,
                ColumnType.StringPointer,
                ColumnType.PagePointer,
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
            out ColumnType[] types,
            out int[][] intColumns,
            out double[][] doubleColumns,
            out long[][] pagePointerColumns,
            out PagePointerOffsetPair[][] pagePointerOffsetColumns,
            int rowCount = 5)
        {
            types = new ColumnType[]
            {
                ColumnType.Int,
                ColumnType.Int,
                ColumnType.Double,
                ColumnType.Int,
                ColumnType.StringPointer,
                ColumnType.PagePointer,
            };

            int intColumnCount = types.Count(t => t == ColumnType.Int);
            int doubleColumnCount = types.Count(t => t == ColumnType.Double);
            int pagePointerOffsetCount = types.Count(t => t == ColumnType.StringPointer);
            int pagePointerCount = types.Count(t => t == ColumnType.PagePointer);

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
