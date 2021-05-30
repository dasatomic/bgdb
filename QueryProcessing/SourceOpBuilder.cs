using MetadataManager;
using PageManager;
using System;
using System.Threading.Tasks;

namespace QueryProcessing
{
    class SourceOpBuilder : IStatementTreeBuilder
    {
        MetadataManager.MetadataManager metadataManager;

        public SourceOpBuilder(MetadataManager.MetadataManager metadataManager)
        {
            this.metadataManager = metadataManager;
        }

        public async Task<IPhysicalOperator<RowHolder>> BuildStatement(Sql.sqlStatement statement, ITransaction tran, IPhysicalOperator<RowHolder> source, InputStringNormalizer stringNormalizer)
        {
            if (source != null)
            {
                // For now source for scan must be null.
                // with subquery expression this will change.
                throw new ArgumentException();
            }

            if (!statement.From.IsFromTable)
            {
                throw new NotImplementedException("Only from table is currently supported");
            }

            string tableName = ((Sql.sqlStatementOrId.FromTable)statement.From).Item;

            MetadataTablesManager tableManager = metadataManager.GetTableManager();
            MetadataTable table = await tableManager.GetByName(tableName, tran).ConfigureAwait(false);

            // Since we currently don't support indexes we can only build scan operation.
            return new PhyOpScan(table.Collection, tran, table.Columns, table.TableName);
        }
    }
}
