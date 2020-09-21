using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using DataStructures;
using MetadataManager;
using Microsoft.FSharp.Core;
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
            // TODO: query builder is currently manual. i.e. SCAN -> optional(FILTER) -> PROJECT.
            // In future we need to build proper algebrizer, relational algebra rules and work on QO.
            string tableName = sqlStatement.Table;
            string[] columns = sqlStatement.Columns.ToArray();

            MetadataTablesManager tableManager = metadataManager.GetTableManager();
            MetadataTable table = await tableManager.GetByName(tableName, tran).ConfigureAwait(false);

            // Scan Op.
            PhyOpScan scanOp = new PhyOpScan(table.Collection, tran);

            // Project Op.
            List<int> columnMapping = new List<int>();
            foreach (string columnName in columns)
            {
                if (!table.Columns.Any(tbl => tbl.ColumnName == columnName))
                {

                    throw new KeyNotFoundException(string.Format("Invalid column name {0}", columnName));
                }

                columnMapping.Add(table.Columns.FirstOrDefault(c => c.ColumnName == columnName).ColumnId);
            }

            // Where op.
            IPhysicalOperator<RowHolderFixed> sourceForProject = scanOp;

            if (FSharpOption<Sql.where>.get_IsSome(sqlStatement.Where))
            {
                Sql.where whereStatement = sqlStatement.Where.Value;
                PhyOpFilter filterOp = new PhyOpFilter(scanOp, FilterStatementBuilder.EvalWhere(whereStatement, table.Columns));
                sourceForProject = filterOp;

            }

            PhyOpProject projectOp = new PhyOpProject(sourceForProject, columnMapping.ToArray());

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
