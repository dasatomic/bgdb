using MetadataManager;
using Microsoft.FSharp.Core;
using PageManager;
using QueryProcessing.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryProcessing
{
    class ProjectOpBuilder : IStatementTreeBuilder
    {
        public Task<IPhysicalOperator<RowHolder>> BuildStatement(Sql.sqlStatement statement, ITransaction tran, IPhysicalOperator<RowHolder> source, InputStringNormalizer inputStringNormalizer)
        {
            Sql.columnSelect[] columns = new Sql.columnSelect[0];
            bool isStar = false;
            string[] projections = new string[0];

            if (statement.GroupBy.Any())
            {
                // no job for me, this is group by.
                return Task.FromResult(source);
            }

            if (!statement.Columns.IsStar)
            {
                columns = (((Sql.selectType.ColumnList)statement.Columns).Item).ToArray();

                if (columns.Any(c => c.IsAggregate == true))
                {
                    // No job for me, this is aggregation.
                    return Task.FromResult(source);
                }

                projections = columns
                    .Where(c => c.IsProjection == true)
                    .Select(c => ((Sql.columnSelect.Projection)c).Item).ToArray();
            }
            else
            {
                isStar = true;
            }

            int? topRows = null;
            if (FSharpOption<int>.get_IsSome(statement.Top))
            {
                topRows = statement.Top.Value;

                if (topRows < 1)
                {
                    throw new InvalidTopCountException();
                }
            }

            if (isStar)
            {
                // no need for project, just return everything.
                IPhysicalOperator<RowHolder> projectOp = new PhyOpProject(source, topRows);
                return Task.FromResult(projectOp);
            }
            else
            {
                // Project Op.
                List<MetadataColumn> columnMapping = new List<MetadataColumn>();
                foreach (string columnName in projections)
                {
                    MetadataColumn mc = QueryProcessingAccessors.GetMetadataColumn(columnName, source.GetOutputColumns());
                    columnMapping.Add(mc);
                }

                IPhysicalOperator<RowHolder> projectOp = new PhyOpProject(source, columnMapping.Select(mc => mc.ColumnId).ToArray(), topRows);
                return Task.FromResult(projectOp);
            }
        }
    }
}
