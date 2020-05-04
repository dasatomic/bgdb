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

        private int[] colIdToValMapper;

        public Row(int[] intCols, double[] doubleCols, string[] stringCols)
        {
            int rowWidth = intCols.Length + doubleCols.Length + stringCols.Length;
            colIdToValMapper = new int[rowWidth];

            for (int i = 0; i < intCols.Length; i++)
            {

            }
        }
    }
}
