using MetadataManager;
using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class CreateTableStatement : ISqlStatement
    {
        private MetadataManager.MetadataManager metadataManager;

        public CreateTableStatement(MetadataManager.MetadataManager metadataManager)
        {
            this.metadataManager = metadataManager;
        }

        public async Task<IEnumerable<Row>> Execute(Sql.DmlDdlSqlStatement statement, ITransaction tran)
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
            tableCreateDefinition.ColumnNames = columns.Select(c => c.Item2).ToArray();
            tableCreateDefinition.ColumnTypes = columns.Select(c =>
            {
                if (c.Item1.IsDoubleCType) return ColumnType.Double;
                else if (c.Item1.IsIntCType) return ColumnType.Int;
                else if (c.Item1.IsStringCType) return ColumnType.StringPointer;
                else throw new ArgumentException();
            }).ToArray();

            tableManager.CreateObject(tableCreateDefinition, tran);

            return Enumerable.Empty<Row>();
        }

        public bool ShouldExecute(Sql.DmlDdlSqlStatement statement) => statement.IsCreate;
    }
}
