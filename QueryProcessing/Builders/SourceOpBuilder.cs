using MetadataManager;
using PageManager;
using QueryProcessing.Exceptions;
using System;
using System.Threading.Tasks;

namespace QueryProcessing
{
    class SourceOpBuilder : IStatementTreeBuilder
    {
        private MetadataManager.MetadataManager metadataManager;
        private AstToOpTreeBuilder nestedStatementBuilder;

        private SourceProvidersSignatures.VideoChunkerProvider videoChunkProvider;

        public SourceOpBuilder(
            MetadataManager.MetadataManager metadataManager,
            AstToOpTreeBuilder nestedStatementBuilder,
            SourceProvidersSignatures.VideoChunkerProvider videoChunkProvider)
        {
            this.metadataManager = metadataManager;
            this.nestedStatementBuilder = nestedStatementBuilder;
            this.videoChunkProvider = videoChunkProvider;
        }

        public void RegisterVideoChunkProvider(SourceProvidersSignatures.VideoChunkerProvider func)
        {
            this.videoChunkProvider = func;
        }

        public async Task<IPhysicalOperator<RowHolder>> BuildStatement(Sql.sqlStatement statement, ITransaction tran, IPhysicalOperator<RowHolder> source, InputStringNormalizer stringNormalizer)
        {
            if (source != null)
            {
                // For now source for scan must be null.
                // with subquery expression this will change.
                throw new ArgumentException();
            }

            if (statement.From.IsFromSubquery)
            {
                Sql.sqlStatement nestedSqlStatement = ((Sql.sqlStatementOrId.FromSubquery)statement.From).Item;
                RowProvider rowProvider = await nestedStatementBuilder.ParseSqlStatement(nestedSqlStatement, tran, stringNormalizer);

                return new PhyOpRowForwarder(rowProvider);
            }
            else if (statement.From.IsFromTable)
            {
                string tableName = ((Sql.sqlStatementOrId.FromTable)statement.From).Item;

                MetadataTablesManager tableManager = metadataManager.GetTableManager();
                MetadataTable table = await tableManager.GetByName(tableName, tran).ConfigureAwait(false);

                // Since we currently don't support indexes we can only build scan operation.
                return new PhyOpScan(table.Collection, tran, table.Columns, table.TableName);
            }
            else if (statement.From.IsFileSystemProvider)
            {
                Sql.value value = ((Sql.sqlStatementOrId.FileSystemProvider)statement.From).Item;

                if (value.IsId)
                {
                    throw new NotImplementedException("File system scan from ID currently not supported");
                }

                if (value.IsString)
                {
                    value = stringNormalizer.ApplyReplacementTokens(value);
                    string path = ((Sql.value.String)value).Item;
                    return new PhyOpFileSystemProvider(path);
                }

                throw new ArgumentException("Invalid argument for FROM FILESYSTEM");
            }
            else if (statement.From.IsVideoChunkProviderSubquery)
            {
                if (this.videoChunkProvider == null)
                {
                    throw new SourceProviderNotSetException();
                }

                Sql.sqlStatement nestedSqlStatement = ((Sql.sqlStatementOrId.VideoChunkProviderSubquery)statement.From).Item1;
                TimeSpan chunkSize = TimeSpan.FromSeconds(((Sql.sqlStatementOrId.VideoChunkProviderSubquery)statement.From).Item2);
                RowProvider rowProvider = await nestedStatementBuilder.ParseSqlStatement(nestedSqlStatement, tran, stringNormalizer);

                return new PhyOpVideoChunker(rowProvider, chunkSize, this.videoChunkProvider);
            }

            throw new ArgumentException("Scan can only be done from Table or from Subquery");
        }
    }
}
