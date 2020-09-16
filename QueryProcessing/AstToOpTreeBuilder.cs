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
        private IAllocateMixedPage allocator;
        private HeapWithOffsets<char[]> stringHeap;

        public AstToOpTreeBuilder(MetadataManager.MetadataManager metadataManager, HeapWithOffsets<char[]> stringHeap, IAllocateMixedPage allocator)
        {
            this.metadataManager = metadataManager;
            this.allocator = allocator;
            this.stringHeap = stringHeap;
        }

        public async Task<IPhysicalOperator<RowHolderFixed>> ParseSqlStatement(Sql.sqlStatement sqlStatement, ITransaction tran)
        {
            string tableName = sqlStatement.Table;
            string[] columns = sqlStatement.Columns.ToArray();

            MetadataTablesManager tableManager = metadataManager.GetTableManager();
            MetadataTable table = await tableManager.GetByName(tableName, tran).ConfigureAwait(false);

            ColumnInfo[] columnTypes = table.Columns.Select(mc => mc.ColumnType).ToArray();

            PageListCollection pcl = new PageListCollection(allocator, columnTypes, table.RootPage);
            PhyOpScan scanOp = new PhyOpScan(pcl, this.stringHeap, tran);

            List<int> columnMapping = new List<int>();
            foreach (string columnName in columns)
            {
                columnMapping.Add(table.Columns.First(c => c.ColumnName == columnName).ColumnId);
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

            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic, tran);
            return op;
        }
    }
}
