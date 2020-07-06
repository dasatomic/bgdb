using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DataStructures;
using LockManager.LockImplementation;
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

        public async Task<IPhysicalOperator<Row>> ParseSqlStatement(Sql.sqlStatement sqlStatement, ITransaction tran)
        {
            string tableName = sqlStatement.Table;
            string[] columns = sqlStatement.Columns.ToArray();

            MetadataTablesManager tableManager = metadataManager.GetTableManager();
            MetadataTable table = await tableManager.GetByName(tableName, tran);

            ColumnType[] columnTypes = table.Columns.Select(x => x.ColumnType).ToArray();

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
            MetadataTable table = await tableManager.GetByName(tableName, tran);

            List<int> intCols = new List<int>();
            List<double> doubleCols = new List<double>();
            List<string> stringCols = new List<string>();

            foreach (var value in insertStatement.Values)
            {
                if (value.IsFloat) doubleCols.Add(((Sql.value.Float)value).Item);
                else if (value.IsInt) intCols.Add(((Sql.value.Int)value).Item);
                else if (value.IsString) stringCols.Add(((Sql.value.String)value).Item);
                else { throw new ArgumentException(); }
            }

            Row[] source = new Row[] { new Row(intCols.ToArray(), doubleCols.ToArray(), stringCols.ToArray() , table.Columns.Select(c => c.ColumnType).ToArray()) };
            PhyOpStaticRowProvider opStatic = new PhyOpStaticRowProvider(source);

            PhyOpTableInsert op = new PhyOpTableInsert(table, allocator, stringHeap, opStatic, tran);
            return op;
        }
    }
}
