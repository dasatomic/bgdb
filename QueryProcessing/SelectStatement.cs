using PageManager;
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

        public async Task<IEnumerable<Row>> Execute(Sql.DmlDdlSqlStatement statement, ITransaction tran)
        {
            if (!statement.IsSelect)
            {
                throw new ArgumentException();
            }

            Sql.DmlDdlSqlStatement.Select selectStatement = ((Sql.DmlDdlSqlStatement.Select)statement);

            IPhysicalOperator<Row> rootOp = this.treeBuilder.ParseSqlStatement(selectStatement.Item, tran);
            return rootOp.Iterate(tran);
        }

        public bool ShouldExecute(Sql.DmlDdlSqlStatement statement) => statement.IsSelect;
    }
}
