using PageManager;
using System;
using System.Collections.Generic;

namespace QueryProcessing
{
    public class InsertIntoTableStatement : ISqlStatement
    {
        private AstToOpTreeBuilder treeBuilder;

        public InsertIntoTableStatement(AstToOpTreeBuilder treeBuilder)
        {
            this.treeBuilder = treeBuilder;
        }

        public async IAsyncEnumerable<RowHolder> Execute(Sql.DmlDdlSqlStatement statement, ITransaction tran)
        {
            if (!statement.IsInsert)
            {
                throw new ArgumentException();
            }

            Sql.DmlDdlSqlStatement.Insert insertStatement = ((Sql.DmlDdlSqlStatement.Insert)statement);
            IPhysicalOperator<RowHolder> rootOp = await this.treeBuilder.ParseInsertStatement(insertStatement.Item, tran).ConfigureAwait(false);
            await rootOp.Invoke().ConfigureAwait(false);

            yield break;
        }

        public bool ShouldExecute(Sql.DmlDdlSqlStatement statement) => statement.IsInsert;
    }
}
