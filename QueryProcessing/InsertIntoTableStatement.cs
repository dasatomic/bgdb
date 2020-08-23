using PageManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QueryProcessing
{
    public class InsertIntoTableStatement : ISqlStatement
    {
        private AstToOpTreeBuilder treeBuilder;

        public InsertIntoTableStatement(AstToOpTreeBuilder treeBuilder)
        {
            this.treeBuilder = treeBuilder;
        }

        public async IAsyncEnumerable<Row> Execute(Sql.DmlDdlSqlStatement statement, ITransaction tran)
        {
            if (!statement.IsInsert)
            {
                throw new ArgumentException();
            }

            Sql.DmlDdlSqlStatement.Insert insertStatement = ((Sql.DmlDdlSqlStatement.Insert)statement);
            IPhysicalOperator<Row> rootOp = await this.treeBuilder.ParseInsertStatement(insertStatement.Item, tran).ConfigureAwait(false);
            await rootOp.Invoke().ConfigureAwait(false);

            yield return null;
        }

        public bool ShouldExecute(Sql.DmlDdlSqlStatement statement) => statement.IsInsert;
    }
}
