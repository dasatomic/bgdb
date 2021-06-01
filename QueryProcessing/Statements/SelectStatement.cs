using PageManager;
using System;
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

        public async Task<RowProvider> BuildTree(Sql.DmlDdlSqlStatement statement, ITransaction tran, InputStringNormalizer stringNormalizer)
        {
            if (!statement.IsSelect)
            {
                throw new ArgumentException();
            }

            Sql.DmlDdlSqlStatement.Select selectStatement = ((Sql.DmlDdlSqlStatement.Select)statement);

            return await this.treeBuilder.ParseSqlStatement(selectStatement.Item, tran, stringNormalizer).ConfigureAwait(false);
        }

        public bool ShouldExecute(Sql.DmlDdlSqlStatement statement) => statement.IsSelect;
    }
}
