using DataStructures;
using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;

namespace QueryProcessing.PhyOperators
{
    public class PhyOpSeek: IPhysicalOperator<RowHolder>
    {
        private readonly IPageCollection<RowHolder> source;
        private readonly ITransaction tran;
        private readonly MetadataColumn[] scanColumnInfo;
        private readonly string collectionName;
        private readonly IEnumerable<RowHolder> seekValues;
        private readonly PhyOpSeekCreator seekCreator;
        private readonly ColumnType seekColumnType;


        public MetadataColumn[] GetOutputColumns() => this.scanColumnInfo;

        public PhyOpSeek(IPageCollection<RowHolder> collection, ITransaction tran, MetadataColumn[] columnInfo, string collectionName, IEnumerable<RowHolder> seekValues, ColumnType seekColumnType)
        {
            this.source = collection;
            this.tran = tran;
            this.collectionName = collectionName;

            this.scanColumnInfo = new MetadataColumn[columnInfo.Length];
            for (int i = 0; i < scanColumnInfo.Length; i++)
            {
                this.scanColumnInfo[i] = new MetadataColumn(
                    scanColumnInfo[i].ColumnId,
                    scanColumnInfo[i].TableId,
                    collectionName + "." + scanColumnInfo[i].ColumnName,
                    scanColumnInfo[i].ColumnType);
            }

            this.seekValues = seekValues;
            this.seekCreator = new PhyOpSeekCreator()
            {
                Source = this.source,
                Tran = this.tran,
                SeekValues = this.seekValues
            };

            this.seekColumnType = seekColumnType;
        }

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            IAsyncEnumerable<RowHolder> seekResults = ColumnTypeHandlerRouter<IAsyncEnumerable<RowHolder>>.Route(
                this.seekCreator,
                this.seekColumnType);
            await foreach (RowHolder rowHolder in seekResults)
            {
                yield return rowHolder; 
            }
        }
    }

    public class PhyOpSeekCreator : ColumnTypeHandlerBasicSingle<IAsyncEnumerable<RowHolder>>
    {
        public IPageCollection<RowHolder> Source { get; init; }
        public ITransaction Tran { get; init; }
        public IEnumerable<RowHolder> SeekValues { get; init; }

        public async IAsyncEnumerable<RowHolder> HandleDouble()
        {
            foreach (RowHolder seekVal in SeekValues)
            {
                await foreach (RowHolder rh in Source.Seek(seekVal.GetField<double>(0), Tran))
                {
                    yield return rh;
                }
            }
        }

        public async IAsyncEnumerable<RowHolder> HandleInt()
        {
            foreach (RowHolder seekVal in SeekValues)
            {
                await foreach (RowHolder rh in Source.Seek(seekVal.GetField<int>(0), Tran))
                {
                    yield return rh;
                }
            }
        }

        public IAsyncEnumerable<RowHolder> HandleString()
        {
            // For strings need to change btee seek signature.
            throw new NotImplementedException();
        }
    }
}
