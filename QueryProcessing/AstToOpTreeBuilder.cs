﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
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

        public IPhysicalOperator<Row> ParseSqlStatement(Sql.sqlStatement sqlStatement)
        {
            string tableName = sqlStatement.Table;
            string[] columns = sqlStatement.Columns.ToArray();

            MetadataTablesManager tableManager = metadataManager.GetTableManager();
            MetadataTable table = tableManager.GetByName(tableName);

            PageListCollection pcl = new PageListCollection(allocator, table.Columns.Select(x => x.ColumnType).ToArray(), allocator.GetMixedPage(table.RootPage));
            PhyOpScan scanOp = new PhyOpScan(pcl, this.stringHeap);

            List<int> columnMapping = new List<int>();
            foreach (string columnName in columns)
            {
                columnMapping.Add(table.Columns.First(c => c.ColumnName == columnName).ColumnId);
            }

            PhyOpProject projectOp = new PhyOpProject(scanOp, columnMapping.ToArray());

            return projectOp;
        }
    }
}
