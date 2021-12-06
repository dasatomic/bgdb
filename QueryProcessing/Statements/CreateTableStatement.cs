using MetadataManager;
using PageManager;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class CreateTableStatement : ISqlStatement
    {
        private MetadataManager.MetadataManager metadataManager;

        private const int MAX_STRING_LENGTH = 128;

        public CreateTableStatement(MetadataManager.MetadataManager metadataManager)
        {
            this.metadataManager = metadataManager;
        }

        public bool ShouldExecute(Sql.DmlDdlSqlStatement statement) => statement.IsCreate;

        public async Task<RowProvider> BuildTree(Sql.DmlDdlSqlStatement statement, ITransaction tran, InputStringNormalizer _)
        {
            if (!statement.IsCreate)
            {
                throw new ArgumentException();
            }

            Sql.DmlDdlSqlStatement.Create createStatement = (Sql.DmlDdlSqlStatement.Create)statement;
            string tableName = createStatement.Item.Table;
            var columns = createStatement.Item.ColumnList.ToList();
            string[] clusteredIndexes = createStatement.Item.ClusteredIndexList.ToArray();

            MetadataTablesManager tableManager = this.metadataManager.GetTableManager();

            TableCreateDefinition tableCreateDefinition = new TableCreateDefinition();
            tableCreateDefinition.TableName = tableName;
            tableCreateDefinition.ColumnNames = columns.Select(c => c.Item3).ToArray();
            tableCreateDefinition.ColumnTypes = columns.Select(c =>
            {
                if (c.Item1.IsDoubleCType)
                {
                    return new ColumnInfo(ColumnType.Double);
                }
                else if (c.Item1.IsIntCType)
                {
                    return new ColumnInfo(ColumnType.Int);
                }
                else if (c.Item1.IsStringCType)
                {
                    if (c.Item2 > MAX_STRING_LENGTH)
                    {
                        throw new ArgumentException("String too big.");
                    }

                    return new ColumnInfo(ColumnType.String, c.Item2);
                }
                else throw new ArgumentException();
            }).ToArray();

            int[] clusteredIndexPositions = new int[clusteredIndexes.Length];
            int posIndex = 0;
            foreach (string clusteredIndexName in clusteredIndexes)
            {
                int posColumn = 0;
                foreach (string columnName in tableCreateDefinition.ColumnNames)
                {
                    if (columnName == clusteredIndexName)
                    {
                        clusteredIndexPositions[posIndex] = posColumn;
                    }
                    posColumn++;
                }

                posIndex++;
            }

            tableCreateDefinition.ClusteredIndexPositions = clusteredIndexPositions;

            await tableManager.CreateObject(tableCreateDefinition, tran).ConfigureAwait(false);

            return new RowProvider(TaskExtension.EmptyEnumerable<RowHolder>(), new MetadataColumn[0]);
        }
    }
}
