using DataStructures;
using MetadataManager;
using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpScan : IPhysicalOperator<Row>
    {
        private PageListCollection source;
        private HeapWithOffsets<char[]> strHeap = null;
        private ITransaction tran;

        public PhyOpScan(PageListCollection collection, HeapWithOffsets<char[]> strHeap, ITransaction tran)
        {
            this.source = collection;
            this.strHeap = strHeap;
            this.tran = tran;
        }

        public async IAsyncEnumerable<Row> Iterate(ITransaction tran)
        {
            // TODO: Need transaction here.
            await foreach (var rowsetHolder in this.source.Iterate(tran))
            {
                foreach (RowHolder rowHolder in rowsetHolder)
                {
                    string[] strVals = new string[rowHolder.strPRow.Length];
                    for (int i = 0; i < strVals.Length; i++)
                    {
                        strVals[i] = new string(await strHeap.Fetch(rowHolder.strPRow[i], this.tran));
                    }

                    Row row = new Row(rowHolder.iRow, rowHolder.dRow, strVals, rowsetHolder.GetColumnTypes());
                    yield return row;
                }
            }
        }

        public Task Invoke()
        {
            throw new NotImplementedException();
        }
    }
}
