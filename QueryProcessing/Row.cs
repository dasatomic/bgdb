using DataStructures;
using PageManager;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class Row : IEquatable<Row>
    {
        private int[] intCols;
        private double[] doubleCols;
        private string[] stringCols;
        private ColumnType[] columnTypes;

        public Row(int[] intCols, double[] doubleCols, string[] stringCols, ColumnType[] columnTypes)
        {
            this.intCols = intCols;
            this.doubleCols = doubleCols;
            this.stringCols = stringCols;
            this.columnTypes = columnTypes;
        }

        public Row Project(int[] columnIds)
        {
            ColumnType[] destColumnTypes = new ColumnType[columnIds.Length];

            int projectPos = 0;
            foreach (int id in columnIds)
            {
                if (id >= this.columnTypes.Length)
                {
                    throw new ArgumentException();
                }

                destColumnTypes[projectPos++] = this.columnTypes[id];
            }

            List<int> iCols = new List<int>();
            List<double> dCols = new List<double>();
            List<string> sCols = new List<string>();

            for (int posProj = 0; posProj < columnIds.Length; posProj++)
            {
                int iColNum = 0;
                int dColNum = 0;
                int sColNum = 0;

                for (int posSource = 0; posSource < columnIds[posProj]; posSource++)
                {
                    if (this.columnTypes[posSource] == ColumnType.Int) iColNum++;
                    else if (this.columnTypes[posSource] == ColumnType.Double) dColNum++;
                    else if (this.columnTypes[posSource] == ColumnType.StringPointer) sColNum++;
                    else throw new InvalidRowsetDefinitionException();
                }

                if (this.columnTypes[columnIds[posProj]] == ColumnType.Int) iCols.Add(this.intCols[iColNum]);
                else if (this.columnTypes[columnIds[posProj]] == ColumnType.Double) dCols.Add(this.doubleCols[dColNum]);
                else if (this.columnTypes[columnIds[posProj]] == ColumnType.StringPointer) sCols.Add(this.stringCols[sColNum]);
                else throw new InvalidRowsetDefinitionException();
            }

            return new Row(iCols.ToArray(), dCols.ToArray(), sCols.ToArray(), destColumnTypes);
        }

        public ColumnType[] ColumnTypesOrdered => this.columnTypes;

        public int[] IntCols => intCols;
        public double[] DoubleCols => doubleCols;
        public string[] StringCols => stringCols;

        public bool Equals([AllowNull] Row other)
        {
            if (other == null)
            {
                return false;
            }

            return Enumerable.SequenceEqual(this.intCols, other.intCols) &&
                Enumerable.SequenceEqual(this.doubleCols, other.doubleCols) &&
                Enumerable.SequenceEqual(this.stringCols, other.stringCols);
        }

        public async Task<RowHolderFixed> ToRowHolderFixed(HeapWithOffsets<char[]> stringAlloc, ITransaction tran)
        {
            RowHolderFixed rhf = new RowHolderFixed(columnTypes);
            int iPos = 0, dPos = 0, sPos = 0;
            int colNum = 0;
            PagePointerOffsetPair[] offsetCols = await PushStringsToStringHeap(stringAlloc, tran).ConfigureAwait(false);

            foreach (ColumnType ct in this.columnTypes)
            {
                if (ct == ColumnType.Double)
                {
                    rhf.SetField<double>(colNum, this.doubleCols[dPos++]);
                } else if (ct == ColumnType.Int)
                {
                    rhf.SetField<int>(colNum, this.intCols[iPos++]);
                } else if (ct == ColumnType.StringPointer)
                {
                    rhf.SetField<PagePointerOffsetPair>(colNum, offsetCols[sPos++]);
                }

                colNum++;
            }

            return rhf;
        }

        private async Task<PagePointerOffsetPair[]> PushStringsToStringHeap(HeapWithOffsets<char[]> stringAloc, ITransaction tran)
        {
            PagePointerOffsetPair[] locs = new PagePointerOffsetPair[stringCols.Length];
            int i = 0;
            foreach (string str in stringCols)
            {
                locs[i++] = await stringAloc.Add(str.ToCharArray(), tran).ConfigureAwait(false);
            }

            return locs;
        }
    }
}
