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

        public async IAsyncEnumerable<RowHolderFixed> Execute(Sql.DmlDdlSqlStatement statement, ITransaction tran)
        {
            if (!statement.IsSelect)
            {
                throw new ArgumentException();
            }

            Sql.DmlDdlSqlStatement.Select selectStatement = ((Sql.DmlDdlSqlStatement.Select)statement);

            IPhysicalOperator<RowHolderFixed> rootOp = await this.treeBuilder.ParseSqlStatement(selectStatement.Item, tran).ConfigureAwait(false);

            await foreach (RowHolderFixed row in rootOp.Iterate(tran))
            {
                yield return row;
            }
        }

        public bool ShouldExecute(Sql.DmlDdlSqlStatement statement) => statement.IsSelect;
    }
}
