using DataStructures;
using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class PhyOpScan : IPhysicalOperator<RowHolder>
    {
        private readonly IPageCollection<RowHolder> source;
        private readonly ITransaction tran;
        private readonly MetadataColumn[] scanColumnInfo;
        private readonly string collectionName;

        public PhyOpScan(IPageCollection<RowHolder> collection, ITransaction tran, MetadataColumn[] scanColumnInfo, string collectionName)
        {
            this.source = collection;
            this.tran = tran;
            this.collectionName = collectionName;

            this.scanColumnInfo = new MetadataColumn[scanColumnInfo.Length];
            for (int i = 0; i < scanColumnInfo.Length; i++)
            {
                this.scanColumnInfo[i] = new MetadataColumn(
                    scanColumnInfo[i].ColumnId,
                    scanColumnInfo[i].TableId,
                    collectionName + "." + scanColumnInfo[i].ColumnName,
                    scanColumnInfo[i].ColumnType);
            }
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            await foreach (RowHolder rowHolder in this.source.Iterate(tran))
            {
                yield return rowHolder;
            }
        }

        public Task Invoke()
        {
            throw new NotImplementedException();
        }

        public MetadataColumn[] GetOutputColumns() => this.scanColumnInfo;
    }
}
