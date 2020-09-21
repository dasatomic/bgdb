using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DataStructures;
using MetadataManager;
using PageManager;

namespace QueryProcessing
{
    public class AstToOpTreeBuilder
    {
        private MetadataManager.MetadataManager metadataManager;

        public AstToOpTreeBuilder(MetadataManager.MetadataManager metadataManager)
        {
            this.metadataManager = metadataManager;
        }

        public async Task<IPhysicalOperator<RowHolderFixed>> ParseSqlStatement(Sql.sqlStatement sqlStatement, ITransaction tran)
        {
            string tableName = sqlStatement.Table;
            string[] columns = sqlStatement.Columns.ToArray();

            MetadataTablesManager tableManager = metadataManager.GetTableManager();
            MetadataTable table = await tableManager.GetByName(tableName, tran).ConfigureAwait(false);
            PhyOpScan scanOp = new PhyOpScan(table.Collection, tran);

            List<int> columnMapping = new List<int>();
            foreach (string columnName in columns)
            {
                if (!table.Columns.Any(tbl => tbl.ColumnName == columnName))
                {

                    throw new KeyNotFoundException(string.Format("Invalid column name {0}", columnName));
                }

                columnMapping.Add(table.Columns.FirstOrDefault(c => c.ColumnName == columnName).ColumnId);
            }

            PhyOpProject projectOp = new PhyOpProject(scanOp, columnMapping.ToArray());

            return projectOp;
        }

        public async Task<PhyOpTableInsert> ParseInsertStatement(Sql.insertStatement insertStatement, ITransaction tran)
        {
            string tableName = insertStatement.Table;

            MetadataTablesManager tableManager = metadataManager.GetTableManager();
            MetadataTable table = await tableManager.GetByName(tableName, tran).ConfigureAwait(false);

            ColumnInfo[] columnInfosFromTable = table.Columns.Select(mt => mt.ColumnType).ToArray();

            RowHolderFixed rowHolder = new RowHolderFixed(columnInfosFromTable);

            int colNum = 0;
            foreach (var value in insertStatement.Values)
            {
                if (value.IsFloat) rowHolder.SetField<double>(colNum, ((Sql.value.Float)value).Item);
                else if (value.IsInt) rowHolder.SetField<int>(colNum, ((Sql.value.Int)value).Item);
                else if (value.IsString) rowHolder.SetField(colNum, ((Sql.value.String)value).Item.ToCharArray());
                else { throw new ArgumentException(); }

                colNum++;
            }

            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(rowHolder);

            PhyOpTableInsert op = new PhyOpTableInsert(table.Collection, opStatic, tran);
            return op;
        }
    }
}
