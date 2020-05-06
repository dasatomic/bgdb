using MetadataManager;
using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;

namespace QueryProcessing
{
    public class PhyOpScan : IPhysicalOperator<Row>
    {
        private PageListCollection source;
        private HeapWithOffsets<char[]> strHeap = null;

        public PhyOpScan(PageListCollection collection, HeapWithOffsets<char[]> strHeap)
        {
            this.source = collection;
            this.strHeap = strHeap;
        }

        public IEnumerator<Row> GetEnumerator()
        {
            foreach (var rowsetHolder in this.source)
            {
                foreach (RowHolder rowHolder in rowsetHolder)
                {
                    string[] strVals = new string[rowHolder.strPRow.Length];
                    for (int i = 0; i < strVals.Length; i++)
                    {
                        strVals[i] = new string(strHeap.Fetch(rowHolder.strPRow[i]));
                    }

                    Row row = new Row(rowHolder.iRow, rowHolder.dRow, strVals, rowsetHolder.GetColumnTypes());
                    yield return row;
                }
            }
        }

        public void Invoke()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator1();
        }

        private IEnumerator GetEnumerator1()
        {
            return this.GetEnumerator();
        }
    }
}
