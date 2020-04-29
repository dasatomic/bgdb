using PageManager;
using System.Linq;

namespace PageManagerTests
{
    static class GenerateDataUtils
    {
        public static void GenerateSampleData(
            out ColumnType[] types,
            out int[][] intColumns,
            out double[][] doubleColumns,
            out PagePointerPair[][] pagePointerColumns)
        {
            types = new ColumnType[]
            {
                ColumnType.Int,
                ColumnType.Int,
                ColumnType.Double,
                ColumnType.Int,
                ColumnType.StringPointer,
            };

            int intColumnCount = types.Count(t => t == ColumnType.Int);
            int doubleColumnCount = types.Count(t => t == ColumnType.Double);
            int pagePointerCount = types.Count(t => t == ColumnType.StringPointer);

            const int rowCount = 5;

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

            pagePointerColumns = new PagePointerPair[pagePointerCount][];
            for (int i = 0; i < pagePointerColumns.Length; i++)
            {
                pagePointerColumns[i] = new PagePointerPair[rowCount];

                for (int j = 0; j < rowCount; j++)
                {
                    pagePointerColumns[i][j].PageId = i;
                    pagePointerColumns[i][j].OffsetInPage = i;
                }
            }
        }
    }
}
