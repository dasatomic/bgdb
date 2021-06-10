using DataStructures;
using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using QueryProcessing.Exceptions;

namespace QueryProcessing
{
    /// <summary>
    /// Static row provider used to fetch results from subquery or different source that exposes rowProvider.
    /// </summary>
    public class PhyOpRowForwarder : IPhysicalOperator<RowHolder>
    {
        private RowProvider rowProvider;

        public PhyOpRowForwarder(RowProvider rowProvider)
        {
            this.rowProvider = rowProvider;
        }

        public MetadataColumn[] GetOutputColumns() => rowProvider.ColumnInfo;

        public IAsyncEnumerable<RowHolder> Iterate(ITransaction tran) => rowProvider.Enumerator;
    }

    public class PhyOpVideoChunker : IPhysicalOperator<RowHolder>
    {
        private RowProvider rowProvider;
        private TimeSpan chunkLength;
        const string FilePathField = "FilePath";

        public PhyOpVideoChunker(RowProvider rowProvider, TimeSpan chunkLength)
        {
            this.rowProvider = rowProvider;
            this.chunkLength = chunkLength;
        }

        public MetadataColumn[] GetOutputColumns() => rowProvider.ColumnInfo;

        public async IAsyncEnumerable<RowHolder> Iterate(ITransaction tran)
        {
            int filePathColumnId = -1;
            foreach (MetadataColumn md in this.rowProvider.ColumnInfo)
            {
                if (md.ColumnName == FilePathField)
                {
                    filePathColumnId = md.ColumnId;

                    if (md.ColumnType.ColumnType != ColumnType.String && md.ColumnType.ColumnType != ColumnType.StringPointer)
                    {
                        throw new FilePathColumnNotStringException();
                    }

                    break;
                }
            }

            if (filePathColumnId == -1)
            {
                throw new FilePathColumnDoesntExist();
            }

            // Need to build projection of all the columns plus append the chunk_name to end.
            ProjectExtendInfo.MappingType[] mappingTypes = new ProjectExtendInfo.MappingType[rowProvider.ColumnInfo.Length + 1];

            for (int i = 0; i < mappingTypes.Length - 1; i++)
            {
                mappingTypes[i] = ProjectExtendInfo.MappingType.Projection;
            }

            mappingTypes[mappingTypes.Length - 1] = ProjectExtendInfo.MappingType.Extension;

            int[] projectSourcePositions = new int[mappingTypes.Length - 1];
            for (int i = 0; i < projectSourcePositions.Length; i++)
            {
                projectSourcePositions[i] = i;
            }

            ColumnInfo[] extendedColumnInfo = new[] { new ColumnInfo(ColumnType.String, 256) };
            ProjectExtendInfo extendInfo = new ProjectExtendInfo(mappingTypes, projectSourcePositions, extendedColumnInfo);

            int chunkNamePosition = mappingTypes.Length - 1;

            await foreach (RowHolder row in rowProvider.Enumerator)
            {
                string filePath = new string(row.GetStringField(filePathColumnId));

                // TODO: just WIP.
                for (int i = 0; i < 10; i++)
                {
                    RowHolder expended = row.ProjectAndExtend(extendInfo);
                    string chunkName = filePath + "00" + i;
                    expended.SetField(chunkNamePosition, chunkName.ToCharArray());

                    yield return expended;
                }
            }
        }
    }

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
