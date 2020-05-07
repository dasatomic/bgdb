using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class SelectStatement : ISqlStatement
    {
        private AstToOpTreeBuilder treeBuilder;

        public SelectStatement(AstToOpTreeBuilder treeBuilder)
        {
            this.treeBuilder = treeBuilder;
        }

        public async Task<IEnumerable<Row>> Execute(Sql.DmlDdlSqlStatement statement)
        {
            if (!statement.IsSelect)
            {
                throw new ArgumentException();
            }

            Sql.DmlDdlSqlStatement.Select selectStatement = ((Sql.DmlDdlSqlStatement.Select)statement);

            IPhysicalOperator<Row> rootOp = this.treeBuilder.ParseSqlStatement(selectStatement.Item);
            return rootOp;
        }

        public bool ShouldExecute(Sql.DmlDdlSqlStatement statement) => statement.IsSelect;
    }
}
