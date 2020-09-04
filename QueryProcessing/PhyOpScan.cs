using DataStructures;
using MetadataManager;
using PageManager;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
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
            await foreach (RowHolderFixed rowHolder in this.source.Iterate(tran))
            {
                // TODO: How to efficiently fetch strings?
                List<int> iCols = new List<int>();
                List<double> dCols = new List<double>();
                List<string> sCols = new List<string>();

                int i = 0;
                foreach (ColumnType ct in source.GetColumnTypes())
                {
                    if (ct == ColumnType.StringPointer)
                    {
                        sCols.Add(new string(await strHeap.Fetch(rowHolder.GetField<PagePointerOffsetPair>(i), tran)));
                    }
                    else if (ct == ColumnType.Int)
                    {
                        iCols.Add(rowHolder.GetField<int>(i));
                    }
                    else if (ct == ColumnType.Double)
                    {
                        dCols.Add(rowHolder.GetField<double>(i));
                    }

                    i++;
                }

                Row row = new Row(iCols.ToArray(), dCols.ToArray(), sCols.ToArray(), source.GetColumnTypes());
                yield return row;
            }
        }

        public Task Invoke()
        {
            throw new NotImplementedException();
        }
    }
}
