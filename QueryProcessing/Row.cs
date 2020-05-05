using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.Text;

namespace QueryProcessing
{
    public class Row
    {
        private int[] intCols;
        private double[] doubleCols;
        private string[] stringCols;

        public Row(int[] intCols, double[] doubleCols, string[] stringCols)
        {
            int rowWidth = intCols.Length + doubleCols.Length + stringCols.Length;

            this.intCols = intCols;
            this.doubleCols = doubleCols;
            this.stringCols = stringCols;
        }

        public RowsetHolder ToRowsetHolder(ColumnType[] columnTypes, HeapWithOffsets<char[]> stringAlloc)
        {
            RowsetHolder rh = new RowsetHolder(columnTypes);

            int[][] intColsPrep = new int[intCols.Length][];
            for (int i = 0; i < intCols.Length; i++)
            {
                intColsPrep[i] = new int[1] { intCols[0] };
            }

            double[][] doubleColsPrep = new double[doubleCols.Length][];
            for (int i = 0; i < doubleCols.Length; i++)
            {
                doubleColsPrep[i] = new double[1] { doubleCols[0] };
            }

            PagePointerOffsetPair[] offsetCols = PushStringsToStringHeap(stringAlloc);
            PagePointerOffsetPair[][] offsetPreps = new PagePointerOffsetPair[offsetCols.Length][];
            for (int i = 0; i < offsetCols.Length; i++)
            {
                offsetPreps[i] = new PagePointerOffsetPair[1] { offsetCols[0] };
            }

            rh.SetColumns(intColsPrep, doubleColsPrep, offsetPreps, new long[0][]);

            return rh;
        }

        private PagePointerOffsetPair[] PushStringsToStringHeap(HeapWithOffsets<char[]> stringAloc)
        {
            PagePointerOffsetPair[] locs = new PagePointerOffsetPair[stringCols.Length];
            int i = 0;
            foreach (string str in stringCols)
            {
                locs[i++] = stringAloc.Add(str.ToCharArray());
            }

            return locs;
        }
    }
}
