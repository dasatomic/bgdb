using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
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
            tableCreateDefinition.ClusteredIndexPositions = new int[] { };

            await tableManager.CreateObject(tableCreateDefinition, tran).ConfigureAwait(false);

            return new RowProvider(TaskExtension.EmptyEnumerable<RowHolder>(), new MetadataColumn[0]);
        }
    }
}
