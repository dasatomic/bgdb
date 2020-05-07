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

        public async Task<IEnumerable<Row>> Execute(Sql.DmlDdlSqlStatement statement)
        {
            if (!statement.IsInsert)
            {
                throw new ArgumentException();
            }

            Sql.DmlDdlSqlStatement.Insert insertStatement = ((Sql.DmlDdlSqlStatement.Insert)statement);
            IPhysicalOperator<Row> rootOp = this.treeBuilder.ParseInsertStatement(insertStatement.Item);
            rootOp.Invoke();

            return Enumerable.Empty<Row>();
        }

        public bool ShouldExecute(Sql.DmlDdlSqlStatement statement) => statement.IsInsert;
    }
}
