using MetadataManager;
using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;

namespace QueryProcessing
{
    public class PhyOpScan : IPhysicalOperator<Row>
    {
        private PageListCollection pageListCollection;
        private IEnumerator<RowsetHolder> enumerator = null;
        private HeapWithOffsets<char[]> strHeap = null;
        int posInRowsetHolder = 0;

        public PhyOpScan(PageListCollection collection, HeapWithOffsets<char[]> strHeap)
        {
            this.pageListCollection = collection;
            this.enumerator = this.pageListCollection.GetEnumerator();
            this.strHeap = strHeap;
        }

        public IEnumerator<Row> GetEnumerator()
        {
            foreach (var rowsetHolder in this.pageListCollection)
            {
                foreach (RowHolder rowHolder in rowsetHolder)
                {
                    string[] strVals = new string[rowHolder.strPRow.Length];
                    for (int i = 0; i < strVals.Length; i++)
                    {
                        strVals[i] = new string(strHeap.Fetch(rowHolder.strPRow[i]));
                    }

                    Row row = new Row(rowHolder.iRow, rowHolder.dRow, strVals);
                    yield return row;
                }
            }
        }

        public void Invoke(IPhysicalOperator<Row> input)
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
