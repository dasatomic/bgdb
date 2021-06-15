using DataStructures;
using MetadataManager;
using PageManager;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using QueryProcessing.Exceptions;
using static QueryProcessing.SourceProvidersSignatures;

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
        private VideoChunkerProvider videoChunkProvider;
        private const string FilePathField = "FilePath";
        private MetadataColumn[] outputColumns;

        private MetadataColumn[] extensionColumns = new[]
        {
            new MetadataColumn(0, 0, "chunk_path", new ColumnInfo(ColumnType.String, 256)), // Chunk path
            new MetadataColumn(1, 0, "NbStreams", new ColumnInfo(ColumnType.Int)), // NbStreams
            new MetadataColumn(2, 0, "NbPrograms", new ColumnInfo(ColumnType.Int)), // NbPrograms,
            new MetadataColumn(3, 0, "StartTimeInSeconds", new ColumnInfo(ColumnType.Double)), // StartTimeInSeconds,
            new MetadataColumn(4, 0, "DurationInSeconds", new ColumnInfo(ColumnType.Double)), // DurationInSeconds,
            new MetadataColumn(5, 0, "FormatName", new ColumnInfo(ColumnType.String, 256)), // Format name,
            new MetadataColumn(6, 0, "BitRate", new ColumnInfo(ColumnType.Int)), // BitRate,
        };

        public PhyOpVideoChunker(RowProvider rowProvider, TimeSpan chunkLength, SourceProvidersSignatures.VideoChunkerProvider videoChunkerCallback)
        {
            this.rowProvider = rowProvider;
            this.chunkLength = chunkLength;
            this.videoChunkProvider = videoChunkerCallback;

            this.outputColumns = new MetadataColumn[rowProvider.ColumnInfo.Length + this.extensionColumns.Length];
            rowProvider.ColumnInfo.CopyTo(outputColumns, 0);

            int extensionPosition = 0;
            for (int i = rowProvider.ColumnInfo.Length; i < outputColumns.Length; i++)
            {
                outputColumns[i] = new MetadataColumn(
                    extensionColumns[extensionPosition].ColumnId + rowProvider.ColumnInfo.Length,
                    extensionColumns[extensionPosition].TableId,
                    extensionColumns[extensionPosition].ColumnName,
                    extensionColumns[extensionPosition].ColumnType);
                extensionPosition++;
            }
        }

        public MetadataColumn[] GetOutputColumns() => this.outputColumns;

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

            ProjectExtendInfo.MappingType[] mappingTypes = new ProjectExtendInfo.MappingType[rowProvider.ColumnInfo.Length + extensionColumns.Length];

            for (int i = 0; i < rowProvider.ColumnInfo.Length; i++)
            {
                mappingTypes[i] = ProjectExtendInfo.MappingType.Projection;
            }

            for (int i = rowProvider.ColumnInfo.Length; i < mappingTypes.Length; i++)
            {
                mappingTypes[i] = ProjectExtendInfo.MappingType.Extension;
            }

            int[] projectSourcePositions = new int[rowProvider.ColumnInfo.Length];
            for (int i = 0; i < projectSourcePositions.Length; i++)
            {
                projectSourcePositions[i] = i;
            }

            ProjectExtendInfo extendInfo = new ProjectExtendInfo(mappingTypes, projectSourcePositions, extensionColumns.Select(ec => ec.ColumnType).ToArray());

            await foreach (RowHolder row in rowProvider.Enumerator)
            {
                string filePath = new string(row.GetStringField(filePathColumnId));

                VideoChunkerResult[] videoChunkerResult = await this.videoChunkProvider(filePath, this.chunkLength, tran);
                foreach (VideoChunkerResult videoChunk in videoChunkerResult)
                {
                    RowHolder expended = row.ProjectAndExtend(extendInfo);
                    expended.SetField(rowProvider.ColumnInfo.Length + 0, videoChunk.ChunkPath.ToCharArray());
                    expended.SetField(rowProvider.ColumnInfo.Length + 1, videoChunk.NbStreams);
                    expended.SetField(rowProvider.ColumnInfo.Length + 2, videoChunk.NbPrograms);
                    expended.SetField(rowProvider.ColumnInfo.Length + 3, videoChunk.StartTimeInSeconds);
                    expended.SetField(rowProvider.ColumnInfo.Length + 4, videoChunk.DurationInSeconds);
                    expended.SetField(rowProvider.ColumnInfo.Length + 5, videoChunk.FormatName.ToCharArray());
                    expended.SetField(rowProvider.ColumnInfo.Length + 6, videoChunk.BitRate);

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
